import re

# 네이버 지도에 각 매장별로 적힌 인스타그램 url을 입력으로 받아,
# 올바른 링크인지 확인하고, 적절한 url을 반환합니다.


def get_instagram_link(input_url):

    wrong_url = ["https://instagram.com", "https://instagram.com/", "http://instagram.com",
                 "http://instagram.com/", "instagram.com", "instagram.com/",
                 "https://www.instagram.com/", "http://www.instagram.com/", "https://www.instagram.com",
                 "http://www.instagram.com"]

    if input_url in wrong_url:
        return None

    # query parameter 제거
    if "?" in input_url:
        input_url = input_url.split("?")[0]

    pattern = r'(instagram\.com/[^/?]+)'

    match = re.search(pattern, input_url)

    if match:
        instagram_url = match.group(1)

        instagram_url = "https://" + instagram_url

        if instagram_url.endswith('/'):
            instagram_url = instagram_url[:-1]
            return instagram_url
        return instagram_url
    else:
        return None
