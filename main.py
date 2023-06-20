from bs4 import BeautifulSoup
import csv
import pandas as pd
import asyncio, aiohttp

headers: dict = {
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",

}

cities: dict = {
    "moscow": "0000073738",
    "saintpetersburg": "0000103664"
}


async def get_page_data(session, page_url: str, page: int, pages_count: str, city_name: str) -> None:
    url_page = 'https://4lapy.ru' + page_url[:-1] + str(page)

    async with session.get(url_page, headers=headers) as response:
        response_text = await response.text()

        soup = BeautifulSoup(response_text, 'lxml')

        products = soup.find_all("div", class_='b-common-item--catalog-item')

        product_id_list: list = []  # для хранения спарсенных данных
        product_name_list: list = []
        product_url_list: list = []
        old_price_list: list = []
        price_promo_list: list = []
        brand_list: list = []
        # списки будут при каждом цикле очищаться и не будут загружать память
        for product in products:
            if product.find("div", class_='b-common-item__info-center-block').find("span",
                                                                                   class_="b-common-item__add-to-cart_text").text.strip() == "В корзину":  # если есть в наличии
                product_id = product['data-product-articul'].strip()
                product_name = product.find("span", class_='b-item-name js-item-name').text
                product_url = 'https://4lapy.ru' + product.find("a")['href']

                try:
                    old_price = product.find_all("li", class_="b-weight-container__item")[1].find("a")[
                        'data-oldprice'].strip()
                    if len(old_price) == 0:
                        raise IndexError
                except IndexError:
                    old_price = "Отсутствует"

                price_promo = product.find("span", class_='b-common-item__bottom_current_price').find("span").text + "₽"
                brand = product.find("span", class_='span-strong').text.strip()

                product_id_list.append(product_id)
                product_name_list.append(product_name)
                product_url_list.append(product_url)
                old_price_list.append(old_price)
                price_promo_list.append(price_promo)
                brand_list.append(brand)

            else:
                continue

            df = pd.DataFrame({'product_id': product_id_list,  # для быстрой работы
                               'product_name': product_name_list,
                               'product_url': product_url_list,
                               'old_price': old_price_list,
                               'price_promo': price_promo_list,
                               'brand': brand_list})

        df.to_csv(f"{city_name}.csv", mode='a', index=False, header=False)
        print(f"Обработана страниц {page}/{pages_count}")


async def tasks_data(city_name: str, city_code: str) -> None:
    with open(f"{city_name}.csv", "w") as file:
        write = csv.writer(file)

        write.writerow(
            (
                "id товара",
                "наименование",
                "ссылка на товар",
                "регулярная цена",
                "промо цена",
                "бренд"
            )
        )

    params: dict = {
        "code": "0000040939"
    }

    url: str = 'https://4lapy.ru/ajax/user/city/set/'
    url2: str = 'https://4lapy.ru/catalog/koshki/korm-koshki/sukhoy/'

    cookies: dict = {"selected_city_code": city_code}

    async with aiohttp.ClientSession(cookies=cookies) as session:
        await session.get(url=url, headers=headers, params=params)
        response = await session.get(url=url2, headers=headers)

        # with open("index_new.html", "w") as file:
        #     file.write(await response.text())

        soup = BeautifulSoup(await response.text(), "lxml")

        pages_count = soup.find("ul", class_="b-pagination__list").find_all("a", class_='b-pagination__link')[
            -2].text.strip()
        page_url = soup.find("ul", class_="b-pagination__list").find_all("a", class_='b-pagination__link')[-2]['href']

        tasks = []

        for page in range(1, int(pages_count) + 1):
            task = asyncio.create_task(get_page_data(session, page_url, page, pages_count, city_name))
            tasks.append(task)

        await asyncio.gather(*tasks)


def main() -> None:
    for city_name, city_code in cities.items():
        asyncio.run(tasks_data(city_name, city_code))


if __name__ == '__main__':
    main()
