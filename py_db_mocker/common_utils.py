

def is_sublist(small_list: list, big_list: list):
    if not small_list or not big_list:
        return False
    for i in small_list:
        if i not in big_list:
            return False
    start = big_list.index(small_list[0])
    sub_list = big_list[start: start + len(small_list)]
    return small_list == sub_list


def main():
    a = [3, 4]
    b = [1, 2, 3, 4, 5]
    assert is_sublist(a, b) is True


if __name__ == '__main__':
    main()
