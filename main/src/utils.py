from os import getenv
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from requests import get, Response
from exceptions import WebpageError, HyperlinksScrapeError, ContentScrapeError
from time import time, sleep
import traceback
from functools import wraps

load_dotenv(dotenv_path="config.env")
WIKI_SEED_URL = getenv("WIKI_SEED_URL")

def trace_unhandled_exceptions(func):
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            print('Exception in '+func.__name__)
            traceback.print_exc()
    return wrapped_func


def get_bytes_from_page(page_url: str):

    """
    Gets the HTML content of the page in bytes, raises a WebpageError if not possible.

    Raises:
        WebpageError: Error is raised if the status code of the get request is not 200 (successfull).

    Returns:
        content (bytes): The HTML content of the page in bytes.
    """

    response: Response = get(url=page_url)

    if response.status_code != 200:
        raise WebpageError(response.status_code)
    
    content: bytes = response.content

    return content

def get_hyperlinks_from_page(content: bytes):

    """
    Gets all the hyperlinks from the page and returns them.

    Args:
        content (bytes): The html content of the page in bytes.

    Returns:
        hyperlinks (list[str]): All the hyperlinks present in the page.
    """

    hyperlinks: list[str] = []

    parsed: BeautifulSoup = BeautifulSoup(content, "html5lib")

    i: BeautifulSoup = None
    
    for i in parsed.find_all(name="a"):

        if i.has_attr("href"):
            
            hyperlinks.append(i["href"])
            
    return hyperlinks

def screen_hyperlinks(hyperlink: str, hyperlinks: list[str]):

    """
    A simple function to screen the hyperlinks for a certain prefix; in this use case, en.wikipedia.org and format the ones which match accordingly.

    Args:
        hyperlink (str): The original hyperlink.
        hyperlinks (list[str]): The list of hyperlinks to screen.

    Returns:
        set[str]: The screened list of hyperlinks. 
    """
    
    screened_hyperlinks: list[str] = [] 
    
    prefix_restriction = "en.wikipedia.org"
    
    for i in hyperlinks:

        if "wikipedia" in i:

            if prefix_restriction in i:

                screened_hyperlinks.append(i)

        elif i.startswith("/wiki"): # /wiki/Madeleine_Slade

            screened_hyperlinks.append("https://"+prefix_restriction+i)

        elif i.startswith("//"):
            
            if prefix_restriction in i:

                screened_hyperlinks.append("https:"+i)
    
    formatted_hyperlinks = ["https:" + i if "https:" not in i else i for i in screened_hyperlinks]

    formatted_hyperlinks = [i for i in formatted_hyperlinks if i != hyperlink and ":" not in i[6:] and "?" not in i and "%" not in i and "#" not in i]
    
    return set(formatted_hyperlinks)

def get_content_from_page(content: bytes):

    """
    Gets content from page using bytes, returns single divided into h2, h3, and text tags.

    Arguments:
        content (bytes): The HTML code of the page in bytes.

    Returns:
        (str): The string of the content of the page divided into h2, h3 and text tags.
    """
    
    parsed: BeautifulSoup = BeautifulSoup(content, "html5lib")
# <h1 class="firstHeading mw-first-heading" <i>Bristol Post</i>
    try:
        current_heading = parsed.find(name="span", attrs={"class":"mw-page-title-main"}).text #was here before, works for most
    except:
        current_heading = parsed.find(name="h1", attrs={"class":"firstHeading mw-first-heading"}).text.lstrip("<i>").rstrip("</i>")
        
    page_content_div_children = [i for i in parsed.find(name="div", attrs={"class": "mw-content-ltr mw-parser-output"}).children if i.name is not None] #thats a list of children
    
    page_content: list[BeautifulSoup] = []

    i: BeautifulSoup = None
    
    for i in page_content_div_children:
        if i.name == "div" and i.has_attr("class") and len(i["class"]) == 2 and i["class"][1] in ["mw-heading2", "mw-heading3"]:
            page_content.append(i) 

        if i.name == "p":
            page_content.append(i)
  
    find:int = -1
    for i in page_content[::-1]:
        if i.name == "p":
            break
        find -= 1

    page_content = page_content[:len(page_content)+find+1]

    combined_content_string = ""

    for i in page_content:

        text_from_element = i.text.strip()

        if text_from_element:

            if i.name == "p":

                temp = "<text>%s</text>" % text_from_element

            if i.name == "div":

                if i["class"][1] == "mw-heading2":

                    temp = "<h2>%s</h2>" % text_from_element

                if i["class"][1] == "mw-heading3":

                    temp = "<h3>%s</h3>" % text_from_element
                    
            combined_content_string += temp

    return (current_heading, combined_content_string)
    



def wait(frequency: float):
    """
    Helper function to return True after waiting (1/frequency) seconds.

    Arguments:
        frequency (float): the frequency of execution of a process.

    """
    sleep(1/frequency)
    return True


def refresh(RATELIMITS: list[int], rate_limits: list[int], last_refreshed_rate_limits: list[float]) -> None:
    current_time = time()

    #refresh per second limit
    if (current_time-last_refreshed_rate_limits[0]) >= 1:
        rate_limits[0] = RATELIMITS[0]
        last_refreshed_rate_limits[0] = current_time

    #refresh per minute limit
    if (current_time - last_refreshed_rate_limits[1]) >= 60:
        rate_limits[1] = RATELIMITS[1]
        last_refreshed_rate_limits[1] = current_time

    #refreh per hour limit 
    if (current_time - last_refreshed_rate_limits[2]) >= 3600: 
        rate_limits[2] = RATELIMITS[2]
        last_refreshed_rate_limits[2] = current_time


def makeCall(rate_limits: list[int]) -> None:

    #make(emulate) API call without checking or blocking
    for i in range(len(rate_limits)):
        rate_limits[i] -= 1

def isValidCall(rate_limits: list[int]) -> bool:

    #check if the call doesn't exceed the rate limits and is valid.
    flag = None
    if (rate_limits[0] > 0 and rate_limits[1] > 0 and rate_limits[2] > 0):
        flag = True
    else:
        flag = False

    return flag
    
def makeBlockingCall(RATELIMITS: list[int], rate_limits: list[int], last_refreshed_rate_limits: list[float]) -> None:
    refresh(RATELIMITS, rate_limits, last_refreshed_rate_limits)

    #classic blocking call, blocks untill the call can be made adhering to the rate limits.
    proceed = isValidCall(rate_limits)
    if not proceed:
        while not isValidCall(rate_limits):
            refresh(RATELIMITS, rate_limits, last_refreshed_rate_limits)
    makeCall(rate_limits)
    return None
