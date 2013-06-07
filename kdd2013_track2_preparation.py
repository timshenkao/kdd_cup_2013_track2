import pandas
from time import time
import psycopg2
import pandas.io.sql as sql
import sys
from nltk.corpus import stopwords
import string
import marshal


STOP_WORDS = stopwords.words() + ['keywords'] + ['key words'] + ['keyword'] + ['key word']


def handle_strings(names_initial):
    """Only words with 2 or more letters, get rid of garbage
    :param names_initial: list of names, titles to clean
    """
    removed_symbols = string.whitespace + string.punctuation + string.digits
    if names_initial:
        names = names_initial
        names = set(names)
        names_handled = []
        for name in names:
            #split and remove symbols
            name = [w.strip(removed_symbols) for w in name.split()]
            #if letters and length > 1
            name = [w for w in name if w.isalpha() and len(w) > 1]
            #if word isn't in stop list
            name = [w for w in name if not (w in STOP_WORDS)]
            names_handled += name
        return names_handled
    else:
        return []


def handle_keywords(keywords_initial):
    """Handle keywords (split, get rid of garbage)
    :param keywords_initial: list of keywords to clean
    """
    removed_symbols = string.whitespace + string.punctuation
    if keywords_initial:
        keywords = keywords_initial
        keywords = set(keywords)
        keywords_handled = []
        for keyword in keywords:
            #split and remove symbols
            keyword = [w.strip(removed_symbols) for w in keyword.split()]
            #if letters and digits, and length > 2
            keyword = [w for w in keyword if w.isalnum() and len(w) > 2]
            #if word isn't in stop list
            keyword = [w for w in keyword if not (w in STOP_WORDS)]
            keywords_handled += keyword
        return keywords_handled
    else:
        return []


def create_author_dict(conn):
    """Create dictionary which contains authors' id's as keys
    :param conn: database connection instance
    """
    author_dict = {}

    #get id's from Author table
    df = sql.read_frame('select * from author;', conn)
    #iterate through id and fill in dictionary
    for i in df.itertuples():
        x = int(i[1])
        if not x in author_dict:
            author_dict[x] = {}
            author_dict[x]['paperid_set'] = []
            author_dict[x]['names_set'] = [i[2].lower()]
            author_dict[x]['names_words_set'] = [i[2].lower()]
            author_dict[x]['affiliation_set'] = [i[3].lower()]
            author_dict[x]['keyword_set'] = []
            author_dict[x]['title_set'] = []
            author_dict[x]['title_words_set'] = []
    return author_dict


def create_extended_dataset(author_dict, conn, dbtable_hdata):
    """
    Read data from database and add to dictionary of authors
    :param author_dict: initially filled in dictionary of authors, keys are authorid's
    :param conn: database connection instance
    :param dbtable_hdata: table's name in database,
    this table contains corrected and aggregated information from the other tables
    """
    t0 = time()
    df = sql.read_frame("select * from " + dbtable_hdata + " ;", conn)
    print("Query completed in %.3f seconds" % (time() - t0))

    # it may look ugly but it's faster and more obvious than creating specific function
    for row in df.itertuples():
        author_dict[row[2]]['paperid_set'] += [int(row[1])]

        c = str(row[3])
        if c != '':
            author_dict[row[2]]['names_set'] += [c.lower()]
            author_dict[row[2]]['names_words_set'] += [c.lower()]

        c = str(row[4])
        if c != '':
            author_dict[row[2]]['affiliation_set'] += [c.lower()]

        c = str(row[11])
        if c != '':
            author_dict[row[2]]['keyword_set'] += [c.lower()]

        c = str(row[5])
        if c != '':
            author_dict[row[2]]['title_set'] += [c.lower()]
            author_dict[row[2]]['title_words_set'] += [c.lower()]

    #remove duplicates and handle strings
    for x, v in author_dict.iteritems():
        c = set(author_dict[x]['paperid_set'])
        author_dict[x]['paperid_set'] = c

        c = set(author_dict[x]['affiliation_set'])
        author_dict[x]['affiliation_set'] = c

        author_dict[x]['keyword_set'] = handle_keywords(author_dict[x]['keyword_set'])
        c = set(author_dict[x]['keyword_set'])
        author_dict[x]['keyword_set'] = c

        c = set(author_dict[x]['title_set'])
        author_dict[x]['title_set'] = c

        author_dict[x]['title_words_set'] = handle_strings(author_dict[x]['title_words_set'])
        c = set(author_dict[x]['title_words_set'])
        author_dict[x]['title_words_set'] = c

        author_dict[x]['names_words_set'] = handle_strings(author_dict[x]['names_words_set'])
        c = set(author_dict[x]['names_words_set'])
        author_dict[x]['names_words_set'] = c

        c = set(author_dict[x]['names_set'])
        author_dict[x]['names_set'] = c

    f = open('kdd2013track2.dump', 'wb')
    t0 = time()
    marshal.dump(author_dict, f)
    f.close()
    print('Marshal dump in %.3f' % (time() - t0))
    return author_dict


if __name__ == '__main__':
    cur_f = __file__.split('/')[-1]
    if len(sys.argv) != 6:
        print >> sys.stderr, 'usage: ' + cur_f + ' <dbname> <dbuser> <dbpassword> <host> <dbtable_handled_data>'
        sys.exit(1)
    else:
        dbname = sys.argv[1]
        dbuser = sys.argv[2]
        dbpassword = sys.argv[3]
        host = sys.argv[4]
        dbtable_hdata = sys.argv[5]

        #connect to database
        try:
            t0 = time()
            t_start = t0
            connection_string = "dbname='" + dbname + "' user='" + dbuser + "' host='" + host + "' password='" + \
                                dbpassword + "'"
            conn = psycopg2.connect(connection_string)
            print('Connected to database in %.3f seconds' % (time() - t0))

            #create dictionary which keys are authors' id's
            t0 = time()
            author_dict = create_author_dict(conn)
            print("Initial authors' dictionary created in %.3f seconds" % (time() - t0))

            #create dataset to work with
            #dbtable_hdata has the followinf format:
            #paperid from table 'PaperAuthor'
            # authorid from table 'PaperAuthor'
            # name from table 'PaperAuthor'
            # affiliation from table 'PaperAuthor'
            # title from table 'Paper'print authorids_dict[author_ids[i]]
            # year from table 'Paper'
            # conferenceid from table 'Paper'
            # conf_fullname from table 'Conference'
            # journalid from table 'Paper'
            # jour_fullname from table 'Journal'
            # keyword from table 'Paper'
            t0 = time()
            extended_dataset = create_extended_dataset(author_dict, conn, dbtable_hdata)
            print('Extended dataset created in %.3f seconds (including query)' % (time() - t0))
            print('TOTAL TIME: %.3f seconds' % (time() - t_start))

        except Exception, e:
            print e