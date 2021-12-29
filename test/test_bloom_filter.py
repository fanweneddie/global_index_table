# to test the gitable build time and search time in different circumstances
# we should run this file in directory build/
import subprocess
import copy

# databases to be tested
dbs = ['/data/WenFan/test_data_10G_new', '/data/WenFan/test_data_50G_new', '/data/WenFan/test_data_100G_new']
# read circumstances
read_cases = ['readrandom', 'readhot']
# the overall data entry number
nums = [90400000, 452000000, 904000000]
# the read data entry number
read = 3000000
# whether to use global index table
# each tuple contains (usage description, use_gitable, bloom_bits, use_file_gran_filter)
gitable_usages = [('no_gitable', 0, -1, 1), ('git_wo_filter', 1, -1, 1), \
                  ('git_w_file_filter', 1, 10, 1), ('git_w_block_filter', 1, 10, 0)]
# the argument list in the command
args = ['sudo', './db_bench', '--benchmarks=stats,', '--num=', '--reads=', '--db=', \
        '--use_existing_db=1', '--use_gitable=', '--bloom_bits=', '--use_file_gran_filter=']
# stores the op_count, build_time and search_time of different databases,
# in different read cases and with different gitable usages:
results = {}

for db, num in zip(dbs, nums):
    # stores the result of a db
    db_results = {}
    for read_case in read_cases:
        # stores the result of a read case
        read_case_results = {}
        for gitable_usage in gitable_usages:
            # update the argument list for this test
            arg_list = copy.deepcopy(args)
            arg_list[2] += read_case                # --benchmarks
            arg_list[3] += str(num)                 # --num
            arg_list[4] += str(read)                # --reads
            arg_list[5] += db                       # --db
            arg_list[7] += str(gitable_usage[1])    # --use_gitable
            arg_list[8] += str(gitable_usage[2])    # --bloom_bits
            arg_list[9] += str(gitable_usage[3])    # --use_file_gran_filter
            # result stores op_count, build_time and search_time in a case
            result = [0, 0, 0]
            # run 3 times and get the average
            for i in range(0, 3):
                out = subprocess.check_output(arg_list).decode('utf-8')
                # get the output line
                line = out.split('\n')[-2]
                temp_result = line.split(' ')[2:]
                # aggregate the result
                for j in range(0, 3):
                    result[j] += float(temp_result[j])
            for j in range(0, 3):
                result[j] /= 3
            # store the result
            read_case_results[gitable_usage[0]] = result

        db_results[read_case] = read_case_results

    results[db] = db_results

# show the result
for db in dbs:
    for read_case in read_cases:
        for gitable_usage in gitable_usages:
            result = results[db][read_case][gitable_usage[0]]
            print(db, read_case, gitable_usage[0], \
                  ', build time', result[1], \
                  'ms, search time', (result[2]/result[0])*100000, 'ms per 100000 ops')
        print('------------------------------------')
    print('*************************************************************************')