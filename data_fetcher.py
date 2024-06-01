import asyncio
import json
from goose3 import Goose
from goose3.configuration import Configuration
import datetime
import datetime
import pytz
import nltk
import re
import requests
import pandas as pd
import os
import traceback
from multiprocessing import Semaphore, Process
import threading


requests_timeout = 10
goose_config = Configuration()
goose_config.strict = True
goose_object = Goose()
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

RETRY_LIMIT = 3


class DataFetcher:
    def __init__(self, number_of_threads=5, max_batch_size=500) -> None:
        self.successful_requests = 0
        self.article_data = []
        self.rejected_urls = []
        self.number_of_processes = number_of_threads
        self.semaphore = Semaphore(self.number_of_processes)
        self.current_save_batch = 0
        self.max_batch_size = max_batch_size
        self.save_path = ""
        self.save_json = True
        self.save_csv = True
        self.total_successful_extracted = 0

    @staticmethod
    def add_punctuation_whitespace(text: str) -> str:
        """
        Add whitespace after punctuation marks and fix edge cases with numbers

        Args:
            text: text to be processed

        Returns:
            text: processed text
        """
        sentences = nltk.sent_tokenize(text)
        for i in range(len(sentences)):
            if sentences[i][-1] not in ".!?":
                sentences[i] += "."
        text = " ".join(sentences)
        pattern = (
            r"(?<=[^\s\d])("
            + "|".join(re.escape(x) for x in ".!?,;:")
            + r")(?=[^\s\d])"
        )
        text = re.sub(pattern, r"\1 ", text)
        # Fix edge case with numbers and currency.
        text = re.sub(r"(\d) (?=\d{3}([^\d]|$))", r"\1,", text)
        # Fix edge case with hyphenated words.
        text = re.sub(r"(\w) - (?=\w)", r"\1-", text)
        # Add whitespace after numbers followed by punctuation marks
        pattern = r"(?<=\d)(\.(?=\D)|\-(?=\D)|/(?=\D))(?!\d)"
        text = re.sub(pattern, r"\1 ", text)
        return text

    @staticmethod
    def is_valid_datetime(article_datetime: datetime.datetime) -> bool:
        """
        Is datetime valid or not? Implemented for incorrect datetime provided
        by Goose for certain URLs.

        Args:
            article_datetime: datetime object

        Returns:
            valid: bool value
        """
        is_valid = False
        if not isinstance(article_datetime, datetime.datetime):
            return is_valid

        year = article_datetime.year
        month = article_datetime.month
        day = article_datetime.day

        if not len(str(year)) == 4 and not year > 2000:
            print(
                f"Validation of datetime failed. Year passed: {year} "
                f"for datetime: {article_datetime}"
            )
            return is_valid
        if not month <= 12 and not month >= 1:
            print(
                f"Validation of datetime failed. Month passed: {month} "
                f"for datetime: {article_datetime}"
            )
            return is_valid
        if not day <= 31 and not day >= 1:
            print(
                f"Validation of datetime failed. Day passed: {day} "
                f"for datetime: {article_datetime}"
            )
            return is_valid
        is_valid = True

        return is_valid

    def get_article_content(self, article: object, url: str, keyword: str) -> dict:
        """_summary_

        Args:
            article (_type_): _description_
            url (_type_): _description_
            keyword (_type_): _description_

        Returns:
            _type_: _description_
        """
        article_dict = {}
        article_content = article.cleaned_text
        if article_content is None or article_content.strip() == "":
            article_content = article.meta_description
        article_content = self.add_punctuation_whitespace(article_content)
        article_title = article.title
        if article_content and article_title:
            # article_title, article_content, multilingual_response = Translator.get_translated_article(
            #     article_title, article_content,
            #     detect_lang_and_translate=True
            # )

            article_dict["Content"] = article_content
            article_dict["URL"] = url
            article_dict["Title"] = article_title
            article_dict["keyword"] = keyword
            # article_content['multilingual_response'] = multilingual_response

            article_publish = article.publish_datetime_utc
            if article_publish is not None and self.is_valid_datetime(article_publish):
                try:
                    article_publish = article_publish.astimezone(pytz.UTC)
                    if article_publish > datetime.datetime.now(datetime.timezone.utc):
                        article_publish = datetime.datetime.now(datetime.timezone.utc)
                    article_dict["Time"] = datetime.datetime.strftime(
                        article_publish, "%Y-%m-%d %H:%M"
                    )
                except Exception:
                    print("exception occurred when parsing article_publish.")
                    article_dict["Time"] = datetime.datetime.strftime(
                        datetime.now(datetime.timezone.utc), "%Y-%m-%d %H:%M"
                    )
            else:
                article_dict["Time"] = datetime.datetime.strftime(
                    datetime.datetime.now(datetime.timezone.utc), "%Y-%m-%d %H:%M"
                )
        return article_dict

    async def make_request(self, url: str, keyword: str) -> None:
        """Fetches data from a given URL

        Args:
            url (_type_): _description_
        """
        try:
            article = goose_object.extract(url=url)
            goose_extracted_content = self.get_article_content(article, url, keyword)
            self.article_data.append(goose_extracted_content)

        except Exception:
            try:
                print("goose extractor error. Retrying!", url, keyword)
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=requests_timeout,
                )
                # Extract news data from the HTML content
                article = goose_object.extract(raw_html=response.text)
                goose_extracted_content = self.get_article_content(
                    article, url, keyword
                )
                with Semaphore(1):
                    self.total_successful_extracted += 1
                self.article_data.append(goose_extracted_content)
                if not goose_extracted_content:
                    print("goose extractor ~ EmptyExtractedContentError", url, keyword)
            except Exception as e:
                traceback.print_exc()
                print("ERROR", f"error in fetching data. Error: {e}", url)
                self.rejected_urls.append({"url": url, "keyword": keyword})
        finally:
            with Semaphore(1):
                if len(self.article_data) >= self.max_batch_size:
                    self.save(self.save_path, self.save_json, self.save_csv)

    def make_request_threaded(self, url: str, keyword: str) -> None:
        """Fetches data from a given URL

        Args:
            url (_type_): _description_
        """
        try:
            article = goose_object.extract(url=url)
            # print(article.)
            goose_extracted_content = self.get_article_content(article, url, keyword)
            self.article_data.append(goose_extracted_content)

        except Exception:
            try:
                print("goose extractor error. Retrying!", url, keyword)
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=requests_timeout,
                )
                # Extract news data from the HTML content
                article = goose_object.extract(raw_html=response.text)
                goose_extracted_content = self.get_article_content(
                    article, url, keyword
                )
                with Semaphore(1):
                    self.total_successful_extracted += 1
                self.article_data.append(goose_extracted_content)
                if not goose_extracted_content:
                    print("goose extractor ~ EmptyExtractedContentError", url, keyword)
            except Exception as e:
                traceback.print_exc()
                print("ERROR", f"error in fetching data. Error: {e}", url)
                self.rejected_urls.append({"url": url, "keyword": keyword})
        finally:
            with Semaphore(1):
                if len(self.article_data) >= self.max_batch_size:
                    self.save(self.save_path, self.save_json, self.save_csv)

    def save(self, file_path, save_json, save_csv):
        data = self.article_data

        if save_json:
            os.makedirs(file_path + "/json", exist_ok=True)
            self.save_json_file(
                data, file_path + "/json/" + str(self.current_save_batch) + ".json"
            )
        if save_csv:
            os.makedirs(file_path + "/csv", exist_ok=True)
            self.save_csv_file(
                data, file_path + "/csv/" + str(self.current_save_batch) + ".xlsx"
            )
        self.current_save_batch += 1
        self.article_data = []

    def save_json_file(self, data, filename):
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)

    def save_csv_file(self, data, filename):
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False)

    async def create_extract_requests(self, article_urls):
        # Create couroutine for each URL
        asyncio.Semaphore(100)
        tasks = []
        for url in article_urls:
            tasks.append(self.make_request(url["link"], url["keyword"]))

        return await asyncio.gather(*tasks)

    def create_requests_multithreaded(self, article_urls):
        semaphore = threading.Semaphore(self.number_of_processes)
        threads = []

        def thread_function(url, keyword):
            with semaphore:
                self.make_request_threaded(url, keyword)

        for urls in article_urls:
            t = threading.Thread(
                target=thread_function, args=(urls["link"], urls["keyword"])
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

    def main(
        self,
        article_urls,
        save_path,
        save_json=True,
        save_csv=True,
        multithreaded=False,
    ):
        """_summary_

        Args:
            keywords (_type_): _description_
        """
        self.save_path = save_path
        self.save_json = save_json
        self.save_csv = save_csv
        if multithreaded:
            print("[INFO] Running in multithreaded mode.")
            self.create_requests_multithreaded(article_urls)
        else:
            print("[INFO] Running in async mode.")
            asyncio.run(self.create_extract_requests(article_urls))

        # Rejected URLs
        print("Length of rejected URLs: ", len(self.rejected_urls))
        save_file_rejected = f"{save_path}/rejected_urls.json"
        self.save_json_file(self.rejected_urls, save_file_rejected)

        print("Length of article extracted: ", self.total_successful_extracted)
        self.save(save_path, save_json, save_csv)
        # save_path = f"{save_path}/scrapped_data"
        # if save_json:
        #     self.save_json(self.article_data, save_path + ".json")

        # if save_csv:
        #     self.save_csv(self.article_data, save_path + ".csv")

        print("Data fetched successfully")


if __name__ == "__main__":
    data_fetcher = DataFetcher()
    now_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    save_path = "./data/run_" + now_datetime
    os.makedirs(save_path, exist_ok=True)
    data_fetcher.main(
        article_urls=[
            {
                "link": "https://amp.newsobserver.com/news/local/education/article288048150.html",
                "keyword": "Gaza war",
            },
        ],
        save_json=False,
        save_csv=False,
        save_path=save_path,
        multithreaded=False,
    )
