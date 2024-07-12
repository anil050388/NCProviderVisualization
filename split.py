import os
import dask.dataframe as dd

# Function to process each chunk file
def process_chunk_file(chunk_number):
    # Path to the current chunk file
    file_path = f'chunk_{chunk_number}.csv'

    # Read the chunk file using Dask, specifying dtype as str to handle mixed types
    df = dd.read_csv(file_path, dtype=str)

    # Specify the number of rows per chunk (adjust based on your needs)
    rows_per_chunk = 250_000  # Adjust this based on your needs

    # Compute the number of rows in the DataFrame
    total_rows = df.shape[0].compute()

    # Calculate the number of partitions
    num_partitions = total_rows // rows_per_chunk + 1

    # Repartition the DataFrame into the specified number of chunks
    df = df.repartition(npartitions=num_partitions)

    # Create a directory for the current chunk if it doesn't exist
    output_dir = f'chunk_{chunk_number}'
    os.makedirs(output_dir, exist_ok=True)

    # Save each partition to a separate CSV file in the respective directory
    for i in range(num_partitions):
        partition = df.get_partition(i).compute()
        chunk_file_path = os.path.join(output_dir, f'chunk{chunk_number}_{i+1}.csv')
        partition.to_csv(chunk_file_path, index=False)
        print(f'Saved {chunk_file_path}')

# Loop through chunk files from 1 to 9
for chunk_number in range(1, 10):
    process_chunk_file(chunk_number)
