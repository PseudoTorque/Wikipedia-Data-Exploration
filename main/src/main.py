from multiprocessing import Process
from multiprocessing.managers import SyncManager

from database import Database
import utils
from models import Models
from exceptions import WebpageError, HyperlinksScrapeError, ContentScrapeError
from time import time
from os import getenv
from dotenv import load_dotenv

load_dotenv(dotenv_path="config.env")
WIKI_SEED_URL = getenv("WIKI_SEED_URL")
RATELIMITS = [int(i) for i in getenv("RATELIMITS").split(",")]
HYPERLINK_BUFFER_SIZE = int(getenv("HYPERLINK_BUFFER_SIZE"))
CONTENT_BUFFER_SIZE = int(getenv("CONTENT_BUFFER_SIZE"))
OVERSEER_FREQUENCY = float(getenv("OVERSEER_FREQUENCY"))
PROCESS_FREQUENCY = float(getenv("PROCESS_FREQUENCY"))
NUM_SCRAPER_PROCESS = int(getenv("NUM_SCRAPER_PROCESSES"))


DATABASE = Database()
DATABASE.createTables()



def insert_seed_url():
    
    """
    Inserts the seed URL into the Database to initiate scraping.
    """
    seed_hyperlink = Models.Hyperlink(
        HYPERLINK=WIKI_SEED_URL,
        ATTEMPTS=0,
        HYPERLINKS_SCRAPED=False,
        CONTENT_SCRAPED=False,
        PARENT_HYPERLINK=None,
        PARENT_PRIORITY=0,
        TIMESTAMP=time()
    )

    with DATABASE.createSession() as session:

        session.add(seed_hyperlink)

        session.commit()


def search_database_for_hyperlink(hyperlink: str):
    
    """
    Checks if a given hyperlink already exists in the database.

    Args:
        hyperlink (str): The hyperlink to search for.

    Returns:
        bool: True if the hyperlink exists, False otherwise.
    """
    with DATABASE.createSession() as session:

        exists = session.query(DATABASE.MODELS.Hyperlink).filter(
            DATABASE.MODELS.Hyperlink.HYPERLINK == hyperlink
        ).first()

        return exists is not None

def add_page_to_pages(pages: list[Models.Page]):
    """
    Bulk inserts a list of Page models into the database.

    Args:
        pages (List[Models.Page]): List of Page objects to insert.
    """
    with DATABASE.createSession() as session:

        session.bulk_save_objects(pages)

        session.commit()

def load_hyperlink_from_hyperlinks(n: int):
    """
    Retrieves a list of uns scraped hyperlinks from the database.

    Args:
        n (int): Number of hyperlinks to retrieve.

    Returns:
        List[Models.Hyperlink]: List of Hyperlink objects.
    """
    with DATABASE.createSession() as session:

        query = session.query(DATABASE.MODELS.Hyperlink).filter(
            (DATABASE.MODELS.Hyperlink.HYPERLINKS_SCRAPED == False) |
            (DATABASE.MODELS.Hyperlink.CONTENT_SCRAPED == False)
        )

        hyperlinks = query.all()

        temp = sorted(hyperlinks[0:n+1], key= lambda x: x.PARENT_PRIORITY, reverse=True)

        query.delete()

        session.commit()

        return temp

def add_hyperlink_to_hyperlinks(hyperlinks: list[Models.Hyperlink]):
    """
    Bulk inserts a list of Hyperlink models into the database.

    Args:
        hyperlinks (List[Models.Hyperlink]): List of Hyperlink objects to insert.
    """
    with DATABASE.createSession() as session:

        session.bulk_save_objects(hyperlinks)
        
        session.commit()

def process(rate_limits: list[int], last_refreshed_rate_limits: list[float], content_buffer: list, hyperlink_buffer: list, scraped_count: int, average_hyperlinks_per_page: float, database_hits: int, buffer_hits: int):
    """
    Scrapes the hyperlinks in hyperlink_buffer, first for child hyperlinks, then for content. Also updates HYPERLINKS_SCRAPED 
    and CONTENT_SCRAPED columns in database.

    Args:
        rate_limits (list[int]): current rate limits for requests, per second, minute, and hour
        last_refreshed_rate_limits (list[float]): timestamps when rate_limits were last refreshed, per second, minute, and hour
        content_buffer (list): buffer of scraped content (to be flushed to database when CONTENT_BUFFER_SIZE reached)
        hyperlink_buffer (list): buffer of scraped hyperlinks that are to be scraped (excess dumped when HYPERLINK_BUFFER_SIZE reached)
        scraped_count (int): total hyperlinks processed so far
        average_hyperlinks_per_page (float): metric
        database_hits (int): metric for matches of hyperlink in database
        buffer_hits (int): metric for matches of hyperlink in buffer
    """    

    while(utils.wait(PROCESS_FREQUENCY)):

        if len(hyperlink_buffer) > 0:

            hyperlink: Models.Hyperlink = hyperlink_buffer.pop(0)

            try:
                
                utils.makeBlockingCall(RATELIMITS, rate_limits, last_refreshed_rate_limits)
                
                data = utils.get_bytes_from_page(hyperlink.HYPERLINK) 
                try:
                    if not hyperlink.HYPERLINKS_SCRAPED:
                        
                        out_links = utils.screen_hyperlinks(hyperlink.HYPERLINK, utils.get_hyperlinks_from_page(data))

                        average_hyperlinks_per_page.value =average_hyperlinks_per_page.value*0.1 + 0.9*len(out_links)

                        new_hyperlinks = []

                        for link in out_links:

                            if not search_database_for_hyperlink(link):

                                new_hyperlink = Models.Hyperlink(
                                    PARENT_HYPERLINK=hyperlink.HYPERLINK,
                                    ATTEMPTS=0,
                                    HYPERLINKS_SCRAPED=False,
                                    CONTENT_SCRAPED=False,
                                    HYPERLINK=link,
                                    PARENT_PRIORITY=len(data),
                                    TIMESTAMP=time()
                                )

                                new_hyperlinks.append(new_hyperlink)

                            else:

                                database_hits.value += 1

                        if new_hyperlinks:

                            for i in new_hyperlinks:

                                hyperlink_buffer.insert(0, i)

                        hyperlink.HYPERLINKS_SCRAPED = True #update in database
                        
                    if not hyperlink.CONTENT_SCRAPED:
                        try: # now content scraping -- add to content buffer
                        
                            heading, content = utils.get_content_from_page(data)
                            
                            out = Models.Page()
                            out.HYPERLINK = hyperlink.HYPERLINK
                            out.TITLE = heading
                            out.HEADING = heading
                            out.CONTENT = content
                            out.TIMESTAMP = time()

                            content_buffer.insert(0, out)

                            hyperlink.CONTENT_SCRAPED = True #update in database

                            scraped_count.value = scraped_count.value + 1
                
                        except Exception as e:
                            print(hyperlink.HYPERLINK, "CONTENT ERROR" + str(e))
                            hyperlink.ATTEMPTS += 1
                        
                except Exception as e:
                    print(hyperlink.HYPERLINK, "HYPERLINKS ERROR" + str(e))
                    hyperlink.ATTEMPTS += 1     
                        
            except Exception as e:
                print(hyperlink.HYPERLINK, "BYTES ERROR" + str(e))
                hyperlink.ATTEMPTS += 1

            
            hyperlink_buffer.append(hyperlink)
          
def overseer(content_buffer: list, hyperlink_buffer: list, scraped_count: int, average_hyperlinks_per_page:float, ratelimits: list[int], database_hits: int, buffer_hits: int):
        """
        Oversees the scraping process - flushes the buffers, implements wait, disposes of open database connections to return
        them to SQLAlchemy pool of connections

        Args:
            content_buffer (list): buffer of content scraped
            hyperlink_buffer (list): buffer of hyperlinks scraped to be scraped
            scraped_count (int): number of pages scraped/processed
            average_hyperlinks_per_page (float): metric
            ratelimits (list[int]): current rate limits
            database_hits (int): metric for matches of hyperlink in database
            buffer_hits (int): metric for matches of hyperlink in buffer

        """
        # content buffer needs to be flushed at max len, hyperlink buffer be dumped if over max len, 
        # -- check content buffer
        # -- flush content buffer
        # same for others
        count = 0

        while(utils.wait(OVERSEER_FREQUENCY)):
            
            if len(content_buffer) > CONTENT_BUFFER_SIZE: # if content buffer too big

                temp = [] 
                
                while len(content_buffer) > 0: #while we haven't emptied it

                    temp.append(content_buffer.pop())

                add_page_to_pages(temp) # saves entire content_buffere (in a temp list of Models.Page) to the database

            
            if len(hyperlink_buffer) == 0:

                temp = load_hyperlink_from_hyperlinks(n=10)

                for i in temp:

                    hyperlink_buffer.append(i)


            if len(hyperlink_buffer) > HYPERLINK_BUFFER_SIZE:

                temp = [] 
                
                while len(hyperlink_buffer) > HYPERLINK_BUFFER_SIZE: #while we haven't reduced it to the size

                    temp.append(hyperlink_buffer.pop())

                add_hyperlink_to_hyperlinks(temp)

            count += 1
            if count >= 15:
                print(
                    f"{len(content_buffer)} -> elements in content buffer.\n",
                    f"{len(hyperlink_buffer)} -> elements in hyperlink buffer.\n",
                    f"{scraped_count.value} -> total hyperlinks processed so far.\n",
                    f"{average_hyperlinks_per_page.value:.2f} -> average hyperlinks per page.\n",
                    f"{ratelimits} -> rate limits.\n",
                    f"{database_hits.value} -> total database hits.\n",
                    f"{buffer_hits.value} -> total buffer hits.\n",
                    f"{database_hits.value / (buffer_hits.value + 1):.2f} -> ratio of database to buffer hits."
                )
                count = 0

            DATABASE.ENGINE.dispose()

class Manager(SyncManager):
    pass
  
class WikiScraper:
    """
    Scraper object with manager for concurrent access by processes.
    """    

    def __init__(self) -> None:

        self.manager = self.spawnManager()

        self.hyperlink_buffer = self.manager.list() # hyperlinks to be scraped

        self.content_buffer = self.manager.list()

        self.rate_limits = self.manager.list(RATELIMITS)

        self.last_refreshed_rate_limits = self.manager.list([time(), time(), time()])

        self.process_list = []

        self.overseer = None

        self.average_hyperlinks_per_page = self.manager.Value("d", 0)

        self.scraped_count = self.manager.Value("i", 0)

        self.buffer_hits = self.manager.Value("d", 0)

        self.database_hits = self.manager.Value("d", 0)

# buffer <> -> P1 -> Links -> Content -> status
#           -> P2 ->        -> Content -> 
#                       1       1

    def spawnManager(self):

        manager = Manager()

        manager.start()

        return manager

    def run(self):

        OVERSEER = Process(target=overseer, args=(self.content_buffer, self.hyperlink_buffer, self.scraped_count, self.average_hyperlinks_per_page, self.rate_limits, self.database_hits, self.buffer_hits))
        OVERSEER.start()
        
        for i in range(NUM_SCRAPER_PROCESS):
            PROCESS = Process(target=process, args=(self.rate_limits, self.last_refreshed_rate_limits, self.content_buffer, self.hyperlink_buffer, self.scraped_count, self.average_hyperlinks_per_page, self.database_hits, self.buffer_hits))
            PROCESS.start()
            self.process_list.append(PROCESS)
        
        self.manager.join()

        

if __name__ == "__main__":
    c = input()
    if "a" in c:
        insert_seed_url()
    else:
        test = WikiScraper()
        test.run()

