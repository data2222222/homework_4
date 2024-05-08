import argparse
from partitioner import partitioning

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--chunk_size', type=int, default=100)
    parser.add_argument('--partitioning_key1', type=str, required=True)
    parser.add_argument('--partitioning_key2', type=str, required=True)
    parser.add_argument('--filename', type=str, default="netflix_titles.csv")
    parser.add_argument('--file_path', type=str, default="output/")
    parser.add_argument('--file_mode', choices=['w', 'a'], default='w')
    parser.add_argument('--encoding', type=str, help="File encoding")

    args = parser.parse_args()

    for tv_chunk in partitioning(args.filename, args.chunk_size, args.partitioning_key1, args.partitioning_key2, args.file_path, args.file_mode, args.encoding):
        print(list(tv_chunk))

if __name__ == "__main__":
    main()
