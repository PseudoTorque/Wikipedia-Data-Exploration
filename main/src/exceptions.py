class WebpageError(Exception):
    """
    Exception type that encompasses all errors related to loading/accessing/reading webpages.

    Args:
        error_code (int): Error code for the exception.
        message (str): Message to be displayed with the error.
    """

    def __init__(self, error_code: int, message: str =""):
        super().__init__(message)
        self.error_code = error_code

    def __str__(self):
        return f"(Error Code: {self.error_code})"
    
class HyperlinksScrapeError(Exception):

    """
    Exception type that encompasses all errors related to scraping the hyperlinks of the page.

    Args:
        message (str): Message to be displayed with the error.
    """


    def __init__(self, message: str ="Error in scraping hyperlinks."):
        super().__init__(message)

    def __str__(self):
        return f"{self.message}"
    
class ContentScrapeError(Exception):

    """
    Exception type that encompasses all errors related to scraping the content of the page.

    Args:
        message (str): Message to be displayed with the error.
    """

    def __init__(self, message: str ="Error in scraping content."):
        super().__init__(message)

    def __str__(self):
        return f"{self.message}"