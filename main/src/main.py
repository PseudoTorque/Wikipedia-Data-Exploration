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
    Inserts the seed URL into the Database. Continue scraping after this.
    
    """    

    out = Models.Hyperlink()
    out.HYPERLINK = WIKI_SEED_URL
    out.ATTEMPTS = 0
    out.HYPERLINKS_SCRAPED = False
    out.CONTENT_SCRAPED = False
    out.PARENT_HYPERLINK = None
    out.PARENT_PRIORITY = 0
    out.TIMESTAMP = time()

    session = DATABASE.createSession()

    session.add(out)

    session.commit()

    session.close()

def search_buffer_for_hyperlink(buffer: list[Models.Hyperlink], hyperlink: str):
    """
    Searches buffer to see if given hyperlink already scraped

    Returns:
        state (bool): True if hyperlink already in buffer.
    """    
    
    for i in range(len(buffer)):
        if buffer[i].HYPERLINK == hyperlink:
            return True
    return False

def search_database_for_hyperlink(hyperlink: str):
    
    """
    Searches database to see if given hyperlink already in database

    Returns:
        state (bool): True if hyperlink already in database.
    """    
    
    state = None

    session = DATABASE.createSession()

    
    query = session.query(DATABASE.MODELS.Hyperlink).filter(DATABASE.MODELS.Hyperlink.HYPERLINK==hyperlink)

    state = False if list(query) == [] else True

    session.close()
    
    return state # false if current hyperlink not in database

def add_page_to_pages(pages: list[Models.Page]):
    """
    Save list of Models.Page to database in table Pages.

    Args:
        pages (list[Models.Page]): list of Models.Page to save to database.
    """    

    session = DATABASE.createSession()

    session.bulk_save_objects(pages)

    session.commit()

    session.close()

def load_hyperlink_from_hyperlinks(n: int):
    """
    Returns list of Models.Hyperlink objects from database (sorted by PARENT_PRIORITY column) which have not been scraped for hyperlinks OR for content.

    Args:
        n (int): number of urls to return

    Returns:
        list[Models.Hyperlink]: List from database, sorted by PARENT_PRIORITY column
    """    
    
    session = DATABASE.createSession()

    query = session.query(DATABASE.MODELS.Hyperlink).filter(DATABASE.MODELS.Hyperlink.HYPERLINKS_SCRAPED == False or DATABASE.MODELS.Hyperlink.CONTENT_SCRAPED == False)
    
    out =  sorted(list(query), key= lambda x: x.PARENT_PRIORITY, reverse=True)[:n+1]

    query.delete()

    session.commit()

    session.close()

    return out

def add_hyperlink_to_hyperlinks(hyperlinks: list[Models.Hyperlink]):
    """
    Saves list of Models.Hyperlink objects to database

    Args:
        hyperlinks (list[Models.Hyperlink]): List of Models.Hyperlink objects to be saved
    """    

    session = DATABASE.createSession()

    session.bulk_save_objects(hyperlinks)

    session.commit()

    session.close()
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
                
                data = utils.get_bytes_from_page(hyperlink.HYPERLINK) #got the bytes
                try:
                    if not hyperlink.HYPERLINKS_SCRAPED:
                        
                        out_links = utils.screen_hyperlinks(hyperlink.HYPERLINK, utils.get_hyperlinks_from_page(data))
                        
                        average_hyperlinks_per_page.value =average_hyperlinks_per_page.value*0.5 + 0.5*len(out_links)

                        for i in out_links: #list of children hyperlinks
                            status = search_buffer_for_hyperlink(hyperlink_buffer, hyperlink)
                            
                            if status:
                                buffer_hits.value = buffer_hits.value + 1

                            if not status:
                                status = search_database_for_hyperlink(i) #check whether child already in database

                                if status:
                                    database_hits.value = database_hits.value + 1
                            if not status: #if not, add child to database and hyperlink buffer.
                                
                                out = Models.Hyperlink()
                                out.PARENT_HYPERLINK = hyperlink.HYPERLINK
                                out.ATTEMPTS = 0
                                out.HYPERLINKS_SCRAPED = False
                                out.CONTENT_SCRAPED = False
                                out.HYPERLINK = i
                                out.PARENT_PRIORITY = len(data)
                                out.TIMESTAMP = time()

                                hyperlink_buffer.insert(0, out)

                                scraped_count.value = scraped_count.value + 1 #change to content block

                        hyperlink.HYPERLINKS_SCRAPED = True #update in database
                        
                    if not hyperlink.CONTENT_SCRAPED and False:
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
            if count == 15:
                print("%d elements in content buffer. \n %d elements in hyperlink buffer \n %d total hyperlinks processed so far. \n %f average hyperlinks per page. \n %s -> rate limits. \n %d -> total database hits. \n %d -> total buffer hits. \n %d -> ratio of database to buffer hits." % (len(content_buffer), len(hyperlink_buffer), scraped_count.value, average_hyperlinks_per_page.value, str(ratelimits), database_hits.value, buffer_hits.value, database_hits.value/(buffer_hits.value+1)))
                
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

