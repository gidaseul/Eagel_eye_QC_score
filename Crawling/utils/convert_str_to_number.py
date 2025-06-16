# 인스타그램에서 100K, 10만 등의 형식으로 표기된 str type의 숫자 데이터를 int type으로 변환해서 반환합니다

def convert_str_to_number(input_str):
    if isinstance(input_str, int):
        return input_str
    elif isinstance(input_str, str):
        # 쉼표와 공백 제거
        input_str = input_str.replace(',', '').replace(' ', '')

        # 단위 확인 및 숫자 변환
        if input_str[-1].isdigit():  # 숫자로 끝나면 단위 변환 없이 직접 변환
            return int(input_str)
        else:
            unit = input_str[-1].upper()
            number = float(input_str[:-1])

            if unit == '만':
                return int(number * 10000)
            elif unit == '억':
                return int(number * 100000000)
            elif unit == 'K':
                return int(number * 1000)
            elif unit == 'M':
                return int(number * 1000000)
            else:
                raise ValueError("Unsupported unit: {}".format(unit))
