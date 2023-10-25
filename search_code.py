import re
import stanza
import sqlite3
con = sqlite3.connect('textbase2.db', check_same_thread=False)
cur = con.cursor()

stanza_pipeline = stanza.Pipeline("ru")


def find_quote(search_word):
    search_word = re.sub(r'"', '', search_word)
    search_words = []
    search_words.append(search_word.lower())
    search_words.append(search_word.lower().title())

    word_find = """
    SELECT *
    FROM words
        JOIN sents_words ON words.word_id = sents_words.word_id
        JOIN sents ON sents_words.sent_id = sents.sent_id
        JOIN texts_sents ON sents.sent_id = texts_sents.sent_id
        JOIN texts ON texts_sents.text_id = texts.text_id
        WHERE word = ?
    """
    all_sent = []
    for i in search_words:
        cur.execute(word_find, (i,))
        for element in cur.fetchall():
            all_sent.append(element)

    return all_sent


def find_tag(search_tag):
    search_tag = search_tag.upper()
    sentences = []

    word_pos_find = """
    SELECT *
    FROM words
        JOIN sents_words ON words.word_id = sents_words.word_id
        JOIN sents ON sents_words.sent_id = sents.sent_id
        JOIN texts_sents ON sents.sent_id = texts_sents.sent_id
        JOIN texts ON texts_sents.text_id = texts.text_id
        WHERE pos = ?
    """
    cur.execute(word_pos_find, (search_tag,))
    all_sent = []
    for element in cur.fetchall():
        all_sent.append(element)

    return all_sent


def find_word(search_word):
    search_word = search_word.lower()
    stanza_proc = stanza_pipeline(search_word)
    search_lemma = [word.lemma for sent in stanza_proc.sentences for word in sent.words]
    sentences = []

    word_lemma_find = """
    SELECT *
    FROM words
        JOIN sents_words ON words.word_id = sents_words.word_id
        JOIN sents ON sents_words.sent_id = sents.sent_id
        JOIN texts_sents ON sents.sent_id = texts_sents.sent_id
        JOIN texts ON texts_sents.text_id = texts.text_id
        WHERE lemma = ?
    """
    for lemma in search_lemma:
        cur.execute(word_lemma_find, (lemma,))

    all_sent = []
    for element in cur.fetchall():
        all_sent.append(element)

    return all_sent


def find_quote_and_tag(search_word_and_tag):

    swt = re.sub('[=|+]',' ',search_word_and_tag)
    swt1 = re.sub(r'\s+', ' ', swt)

    if '=' in search_word_and_tag:
        words = []
        search = swt1.split(' ')

        words.append(search[0].lower())
        words.append(search[0].lower().title())
        tag = search[1].upper()

        word_and_tag_find = """
        SELECT *
        FROM words
            JOIN sents_words ON words.word_id = sents_words.word_id
            JOIN sents ON sents_words.sent_id = sents.sent_id
            JOIN texts_sents ON sents.sent_id = texts_sents.sent_id
            JOIN texts ON texts_sents.text_id = texts.text_id
            WHERE word = ? AND pos = ?
        """

        for i in words:
            cur.execute(word_and_tag_find, (i, tag))

        all_sent = []
        for element in cur.fetchall():
            all_sent.append(element)

    elif '+' in search_word_and_tag:
          search = swt1.split(' ')
          search_word = search[0].lower()
          stanza_proc = stanza_pipeline(search_word)
          search_lemma = [word.lemma for sent in stanza_proc.sentences for word in sent.words]

          tag = search[1].upper()


          word_and_tag_find = """
          SELECT *
          FROM words
              JOIN sents_words ON words.word_id = sents_words.word_id
              JOIN sents ON sents_words.sent_id = sents.sent_id
              JOIN texts_sents ON sents.sent_id = texts_sents.sent_id
              JOIN texts ON texts_sents.text_id = texts.text_id
              WHERE lemma = ? AND pos = ?
          """
          all_sent = []
          for lemma in search_lemma:
              cur.execute(word_and_tag_find, (lemma, tag))
              for element in cur.fetchall():
                  all_sent.append(element)

    return all_sent


def choose_func(part):
    result_tag = re.search(r'[A-Za-z]', part)

    if '+' in part or '=' in part:
        return find_quote_and_tag(part)
    elif result_tag:
        return find_tag(part)
    elif '"' in part:
        return find_quote(part)
    elif not result_tag:
        return find_word(part)


def search(line_search):
    line = line_search.split()

    if len(line) == 1:
        dict_meta = {}
        elem = choose_func(line[0])
        final_sent = []

        for i in range(0, len(elem)):
            dict_meta[elem[i][8]] = [elem[i][1], elem[i][16], elem[i][13], elem[i][15], elem[i][14]]
        for sent, meta in dict_meta.items():
            res_sent = f'{sent}'
            res_meta = f'{meta[1]}, «{meta[2]}», {meta[3]}, {meta[4]}'
            final_sent.append(res_sent)
            final_sent.append(res_meta)

        return final_sent

    elif len(line) > 1 and len(line) < 4:
        sentences_print = []  # предложения, которые будем выводить

        sw1 = {}  # словари индекс предложения - индексы слов
        sw2 = {}
        sentences_1 = {}  # множество предложений по первой части запроса (там точно будет нужное в итоге)
        dict_meta = {}
        sw_1_2 = {}  # словарь индекс предложения для триграмм - индекс второго слова в триграмме

        first_word = choose_func(line[0])  # результат по первому слову
        second_word = choose_func(line[1])  # результат по второму слову

        for i in range(0, len(first_word)):  # заполнение списков
            sentences_1[first_word[i][5]] = first_word[i][8]
            dict_meta[first_word[i][8]] = [first_word[i][1], first_word[i][16], first_word[i][13], first_word[i][15],
                                           first_word[i][14]]
            if first_word[i][5] not in sw1:
                sw1[first_word[i][5]] = []
                sw1[first_word[i][5]].append(first_word[i][0])
            else:
                sw1[first_word[i][5]].append(first_word[i][0])
        for i in range(0, len(second_word)):
            if second_word[i][5] not in sw2:
                sw2[second_word[i][5]] = []
                sw2[second_word[i][5]].append(second_word[i][0])
            else:
                sw2[second_word[i][5]].append(second_word[i][0])

        ind_fin = {}  # словарь с индексами предложений, где есть результаты по каждой из частей запроса
        for ind in sw1:
            if ind in sw2:
                ind_fin[ind] = []
                ind_fin[ind].append(sw1[ind])
                ind_fin[ind].append(sw2[ind])

        for ind in ind_fin:
            for ind_1 in ind_fin[ind][0]:
                for ind_2 in ind_fin[ind][1]:
                    if ind_1 + 1 == ind_2:
                        sentences_print.append(sentences_1[ind])
                        sw_1_2[ind] = [ind_2]

        if len(line) == 3:
            sw3 = {}
            third_word = choose_func(line[2])  # результат по третьему слову
            ind_fin_3 = {}
            sentences_print = []

            for i in range(0, len(third_word)):
                if third_word[i][5] not in sw3:
                    sw3[third_word[i][5]] = []
                    sw3[third_word[i][5]].append(third_word[i][0])
                else:
                    sw3[third_word[i][5]].append(third_word[i][0])

            for ind in sw_1_2:
                if ind in sw3:
                    ind_fin_3[ind] = []
                    ind_fin_3[ind].append(sw_1_2[ind])
                    ind_fin_3[ind].append(sw3[ind])

            for ind in ind_fin_3:
                for ind_2 in ind_fin_3[ind][0]:
                    for ind_3 in ind_fin_3[ind][1]:
                        if ind_2 + 1 == ind_3:
                            sentences_print.append(sentences_1[ind])
        final_sent = []
        for sent, meta in dict_meta.items():
            for item in sentences_print:
                if sent == item:
                    res_sent = f'{sent}'
                    res_meta = f'{meta[1]}, «{meta[2]}», {meta[3]}, {meta[4]}'
                    final_sent.append(res_sent)
                    final_sent.append(res_meta)
        return final_sent

    else:
        return 'Некорректный ввод. Наш и Ваш максимум это 3 единицы в строке!'

def check(item):
    if type(item) == list:
        c=1
        return c
    elif type(item) == str:
        c=0
        return c

