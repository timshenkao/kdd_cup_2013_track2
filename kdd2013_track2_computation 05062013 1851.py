from time import time, sleep
import sys
import os
import marshal
from multiprocessing import Process, JoinableQueue, cpu_count, current_process


def tanimoto_similarity(set1, set2):
    """
    Function calculates Tanimoto similarity between set1 and set2
    :param set1: set #1 to compare
    :param set2: set #2 to compare
    """
    intersect = set1 & set2
    if intersect:
        union = set1 | set2
        result = float(len(intersect)) / len(union)
        return result
    else:
        return 0


def calculate_similarity(v1, v2):
    """
    Function calculates linear combination of Tanimoto similarities.
    If one of the sets is empty, then Tanimoto similarity is zero.
    So, we can diminish the amount of computations checking if both of the sets are non-empty
    :param v1: dictionary values for author #1
    :param v2: dictionary values for author #2
    """
    result = 0

    if v1['paperid_set'] and v2['paperid_set']:
        result += 0.08 * tanimoto_similarity(v1['paperid_set'], v2['paperid_set'])

    if v1['names_set'] and v2['names_set']:
        result += 0.30 * tanimoto_similarity(v1['names_set'], v2['names_set'])

    if v1['names_words_set'] and v2['names_words_set']:
        result += 0.26 * tanimoto_similarity(v1['names_words_set'], v2['names_words_set'])

    if v1['title_set'] and v2['title_set']:
        result += 0.12 * tanimoto_similarity(v1['title_set'], v2['title_set'])

    if v1['title_words_set'] and v2['title_words_set']:
        result += 0.08 * tanimoto_similarity(v1['title_words_set'], v2['title_words_set'])

    if v1['affiliation_set'] and v2['affiliation_set']:
        result += 0.08 * tanimoto_similarity(v1['affiliation_set'], v2['affiliation_set'])

    if v1['keyword_set'] and v2['keyword_set']:
        result += 0.08 * tanimoto_similarity(v1['keyword_set'], v2['keyword_set'])
    return result


def create_submission_csv(dataset, output_file='submission_track2.csv'):
    """ Function creates submission csv file for kdd2013 track2
    :param dataset: results of the computation
    :param output_file: name of csv file where to store the results
    """
    f = open(output_file, 'w')
    f.write('AuthorId,DuplicateAuthorIds\n')
    #walk through the data set and create a string for each authorid
    for k, v in dataset.iteritems():
        dup_authors_string = ''
        for elem in v:
            dup_authors_string += ' ' + str(elem)
        f.write(str(k) + ',' + str(k) + dup_authors_string + '\n')


def divide_for_cpu_units(num_authors, cpu_units):
    """ Function divides interval [0, number_of_authorid] into subintervals
    so that every cpu unit will get equal number of computations
    :param num_authors: the number of the authors
    :param cpu_units:  number of cpu units (actually processes to run)
    """
    lst = []
    k = 0
    for i in range(cpu_units):
        sum_intermediate = 0
        #sum of arithmetic progression divided by number of the cpu units
        while sum_intermediate <= (num_authors - 1) * num_authors / (2 * cpu_units):
            sum_intermediate += k
            k += 1
            #if at least one interval has been calculated, use its boundary
        # and take measure to avoid negative boundaries
        if lst:
            z = lst[-1][0]
            lst.append((0 if (num_authors - k + 1) < 0 else (num_authors - k + 1), z - 1))
        #calculate the first interval
        else:
            lst.append((num_authors - k + 2, num_authors))
    return sorted(lst)


def find_similar_authors(extended_dataset, begin, end, maxrows, queue):
    """
    Function calculates similarity between each pair of authorid.
    If similarity is greater than threshold, we add corresponding
    authorid's to resulting dictionary.
    This function is executed in multiprocessing way
    :param extended_dataset: data set from dump  with extended information on authors
    :param begin: lower boundary of authorid interval
    :param end: upper boundary of authorid interval
    :param maxrows: maximal number of rows in data set to handle
    :param queue: queue for IPC
    """
    print('Beginning handling: process %d' % current_process().pid)
    t0 = time()
    #we need to access authorid in order
    author_ids = sorted(extended_dataset.keys())
    #resulting dictionary
    authorids_dict = {}
    for authorid in extended_dataset.keys():
        if not (authorid in authorids_dict):
            authorids_dict[authorid] = []

    for i in range(begin, end):
        for j in range(i + 1, maxrows):
            sim = calculate_similarity(extended_dataset[author_ids[i]], extended_dataset[author_ids[j]])

            if sim > 0.5:
                authorids_dict[author_ids[i]].append(author_ids[j])
                authorids_dict[author_ids[j]].append(author_ids[i])

    print('pid:%d, %.3f, begin:%d, end:%d' % (os.getpid(), time() - t0, begin, end))
    t0 = time()
    #enqueue the result
    queue.put(authorids_dict)
    print('Put in queue in %.3f' % (time() - t0))
    print('Finishing handling: process %d' % current_process().pid)


def multi_process(dataset, cpu_units):
    """
    Multiprocessing is here
    :param dataset: data set from dump  with extended information on authors
    :param cpu_units: number of cpu units (actually processes to run)
    """
    intervals = divide_for_cpu_units(len(dataset), cpu_units)
    process_list = []
    similarities = []
    #queue for results
    q = JoinableQueue()
    for i in range(cpu_units):
        p = Process(target=find_similar_authors, args=(dataset, intervals[i][0], intervals[i][1], len(dataset), q))
        process_list.append(p)
    for p in process_list:
        p.start()
    while q.qsize() < cpu_units:
        sleep(1)
    #get the results from queue
    while q.qsize() > 0:
        try:
            queue_data = q.get()
            if len(queue_data) > 0:
                print('Getting data from queue: process  %d' % current_process().pid)
                similarities.append(queue_data)
                print('Results gathered %d' % len(similarities))
            else:
                break
        except:
            print 'Queue is empty'
            break

    for p in process_list:
        print('Joining process %d' % p.pid)
        p.join()
    return similarities


if __name__ == '__main__':
    cur_f = __file__.split('/')[-1]
    if len(sys.argv) != 2:
        print >> sys.stderr, 'usage: ' + cur_f + ' <dump>'
        sys.exit(1)
    else:
        #read prepared dictionary from dump
        try:
            cpu_units = cpu_count()
            t0 = time()
            t_start = t0
            dump = open(sys.argv[1], 'rb')
            dataset = marshal.load(dump)
            print('Dump loaded in %.3f seconds' % (time() - t0))

            #calculate pairwise similarities in parallel, use several processes
            t0 = time()
            sim = []
            print('Main process: %d' % current_process().pid)
            sim = multi_process(dataset, cpu_units)
            print('Similar authors found in %.3f seconds' % (time() - t0))

            #merging dictionaries together
            t0 = time()
            for d in range(1, cpu_units):
                for m in sim[0].keys():
                    #two lists are added
                    sim[0][m] += sim[d][m]

            #get rid of repeating id's
            for m in sim[0].keys():
                sim[0][m] = list(set(sim[0][m]))
            print('Resulting dictionaries united in %.3f seconds' % (time() - t0))

            t0 = time()
            create_submission_csv(sim[0])
            print('Submission csv-file created in %.3f seconds' % (time() - t0))

            print('TOTAL TIME: %.3f seconds' % (time() - t_start))

        except Exception, e:
            print e