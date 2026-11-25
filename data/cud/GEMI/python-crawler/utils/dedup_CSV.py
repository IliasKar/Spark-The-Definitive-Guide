import csv

def dedup_csv(input_file, output_file):
    """De-duplicate a CSV file and save to new file."""
    seen = set()
    
    with open(input_file, 'r') as in_csv, open(output_file, 'w', newline='') as out_csv:
        reader = csv.reader(in_csv)
        writer = csv.writer(out_csv)
        
        # Write headers
        headers = next(reader)
        writer.writerow(headers)
        seen = []
        for row in reader:            
            # Check if row is duplicate
            if row not in seen:
                seen.append(row)
                writer.writerow(row)

#de-duplicates all the gemi numbers crawled so far
#dedup_csv("./data/all_gemi_codes.csv","./data/all_gemi_codes_deduped.csv")

'''
with open('./data/all_gemi_codes_sample.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in spamreader:
        print(', '.join(row))

with open('./data/all_gemi_codes_sample_deduped.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])
    spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])
'''    