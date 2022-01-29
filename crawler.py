import logging
from pathlib import Path
from typing import Any, Final, List
import requests
from bs4 import BeautifulSoup
import re

from mixin import LoggingMixin
from parallel import Parallel


class CrawlerBase(LoggingMixin):
    """contains base methods for crawling"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._set_logger_level_from_env(
            logger=self.logger,
            default_level=logging.DEBUG,
            env_var_name_of_level="BASE_CRAWLER_LOGGER_LEVEL",
        )

    def crawl_binary_file(
        self,
        url: str,
        download_dir: Path,
        ignore_duplicate: bool = True,
    ) -> None:
        """downloads a binary file from a url and saves it to a directory

        Parameters
        ----------
        url : str
            url to download
        download_dir : Path
            directory to save the file to
        ignore_duplicate : bool
            if True, will not download a file if it already exists in the directory
        """
        if not download_dir.is_dir():
            self.logger.info(f"creating directory {download_dir}")
            download_dir.mkdir(parents=True)

        filepath = download_dir / url.split("/")[-1]
        if ignore_duplicate and filepath.exists():
            self.logger.info(f"{filepath} already exists.")
            return

        request = requests.get(url)
        unique_filepath = self._get_unique_filepath(filepath)
        with open(unique_filepath, "wb") as f:
            f.write(request.content)
            self.logger.info(f"Downloaded {unique_filepath}")

    def _get_unique_filepath(self, path: Path) -> Path:
        unique_path = path
        i = 1
        while unique_path.exists():
            unique_path = unique_path.with_name(path.stem + f"_{i}" + path.suffix)
            i += 1
        return unique_path


class OIBCrawler(CrawlerBase):
    """A crawler class for the Online interlinear Bible
    target page = https://www.scripture4all.org/
    """

    URL_BASE: Final = "https://www.scripture4all.org/OnlineInterlinear/"
    URL_HEBREW_INDEX: Final = URL_BASE + "Hebrew_Index.htm"
    URL_GREEK_INDEX: Final = URL_BASE + "Greek_Index.htm"

    PATTERN_OTPDF_URL: Final = re.compile(r"^OTpdf/.+[.]pdf$")
    PATTERN_NTPDF_URL: Final = re.compile(r"^NTpdf/.+[.]pdf$")

    DOWNLOAD_DIR: Final = Path(__file__).parent / "downloads"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._set_logger_level_from_env(
            logger=self.logger,
            default_level=logging.DEBUG,
            env_var_name_of_level="OIB_CRAWLER_LOGGER_LEVEL",
        )

    def _is_OTpdf_url(self, href: str) -> bool:
        return href and self.PATTERN_OTPDF_URL.match(href)

    def _is_NTpdf_url(self, href: str) -> bool:
        return href and self.PATTERN_NTPDF_URL.match(href)

    def _crawl_hrefs_OTpdf(self) -> List:
        request = requests.get(self.URL_HEBREW_INDEX)
        soup = BeautifulSoup(request.text, "lxml")

        a_OTpdf = soup.find_all("a", href=self._is_OTpdf_url)
        hrefs_OTpdf = [a.attrs["href"] for a in a_OTpdf]

        return hrefs_OTpdf

    def _crawl_hrefs_NTpdf(self) -> List:
        request = requests.get(self.URL_GREEK_INDEX)
        soup = BeautifulSoup(request.text, "lxml")

        a_NTpdf = soup.find_all("a", href=self._is_NTpdf_url)
        hrefs_NTpdf = [a.attrs["href"] for a in a_NTpdf]

        return hrefs_NTpdf

    def crawl(self, download_dir: Path = DOWNLOAD_DIR) -> None:
        """crawls the Online Interlinear Bible

        Parameters
        ----------
        download_dir : Path, optional
            directory to save the file to, by default DOWNLOAD_DIR
        """
        self.logger.info("Crawling the Online interlinear Bible")

        hrefs_OTpdf = self._crawl_hrefs_OTpdf()
        hrefs_NTpdf = self._crawl_hrefs_NTpdf()

        for href in hrefs_OTpdf:
            self.crawl_binary_file(
                url=self.URL_BASE + href,
                download_dir=download_dir / "OTpdf",
            )
        for href in hrefs_NTpdf:
            self.crawl_binary_file(
                url=self.URL_BASE + href,
                download_dir=download_dir / "NTpdf",
            )

        self.logger.info("Finished crawling the Online interlinear Bible")

    def crawl_parallel(self, download_dir: Path = DOWNLOAD_DIR) -> None:
        """crawls the Online Interlinear Bible in parallel

        Parameters
        ----------
        download_dir : Path, optional
            directory to save the file to, by default DOWNLOAD_DIR
        """
        self.logger.info("Crawling the Online interlinear Bible in parallel")

        hrefs_OTpdf = self._crawl_hrefs_OTpdf()
        hrefs_NTpdf = self._crawl_hrefs_NTpdf()

        kwargs_list = [
            {
                "url": self.URL_BASE + href,
                "download_dir": download_dir / "OTpdf",
            }
            for href in hrefs_OTpdf
        ]
        kwargs_list += [
            {
                "url": self.URL_BASE + href,
                "download_dir": download_dir / "NTpdf",
            }
            for href in hrefs_NTpdf
        ]
        parallel = Parallel(func=self.crawl_binary_file, kwargs_list=kwargs_list)
        parallel.run(interval_secs=0.05)

        self.logger.info("Finished crawling the Online interlinear Bible in parallel")
