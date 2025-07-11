import time

import scrapy
from scrapy.downloadermiddlewares.retry import get_retry_request

class RestaurantsSpider(scrapy.Spider):
    name = "restaurants"
    start_urls = ["https://www.healthspace.com/Clients/Missouri/StLouis/St_Louis_Web_Live.nsf/Food-WardList?OpenView&Count=999&"]

    def parse(self, response):
        for link in response.css("a"):
            ward = link.css("::text").get("").strip()
            yield response.follow(link, self.parse_ward, cb_kwargs={"ward": ward})

    def parse_ward(self, response, ward):
        for link in response.css("a::attr(href)"):
            if "Food-FacilityHistory" in link.get():
                yield response.follow(link, self.parse_facility, cb_kwargs={"ward": ward})
            elif "Food-Ward-ByName" in link.get():
                yield response.follow(link, self.parse_ward, cb_kwargs={"ward": ward})

    def parse_facility(self, response, ward):
        facility = dict(name="", location="", kind="", phone_number="", ward=ward)
        for row in response.css("tr"):
            key = row.css("td:nth-child(1)::text").get("").strip()
            value = row.css("td:nth-child(2)::text").get("").strip()
            if "Facility Name" in key:
                facility["name"] = value
            elif "Facility Location" in key: 
                facility["location"] = value
            elif "Facility Type" in key:
                facility["kind"] = value
            elif "Phone Number" in key:
                facility["phone_number"] = value
        if not facility["name"]:
            # longer sleep here to avoid rate limiting
            time.sleep(1)
            yield get_retry_request(response.request, spider=self, max_retry_times=5, reason=f"fields missing from facility: {facility}")
        time.sleep(0.1)
        yield facility

def main():
    RestaurantsSpider()


if __name__ == "__main__":
    main()
