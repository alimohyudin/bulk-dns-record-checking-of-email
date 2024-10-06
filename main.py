import dns.resolver
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

header_row = []
black_list_domains = []
white_list_domains = ['comcast.net']
ignored_list_domains = []

# Function to get MX records for a domain
def get_mx_records(domain):
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        return [str(r.exchange).rstrip('.') for r in answers]
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout):
        return None

# Function to check if MX records include Proofpoint or Rackspace
def is_filtered(mx_records):
    if mx_records is None:
        return False
    
    #print(mx_records)
    for record in mx_records:
        domains = [
            'pphosted.com', 'emailsrvr.com', 'beccaria.at', 'wwaglobal.com', 'wdf-company.com',
            'tuckstrucks.com', 'trezo.com', 'progressive4.life', 'misbahmc.ae', 'kimha.com', 
            'jb1937.com', 'hofflerandassociates.com', 'eco-recitec.com.br', 
            'corporacionculturalluterana.org', 'fmtest-stag.com'
        ]

        if any(domain in record.lower() for domain in domains):
            return True
    return False

# Function to process a single domain and filter out unwanted ones
def process_row(row):
    email = row[5]
    # get domain from email
    domain = ''
    try:
        domain = email.split("@")[1]
        if(domain in black_list_domains):
            return None 
        if(domain in white_list_domains):
            return row
        if(domain in ignored_list_domains):
            return None

        mx_records = get_mx_records(domain)
        #print(mx_records)
        if not is_filtered(mx_records):
            white_list_domains.append(domain)
            return row  # Return the domain if it's not using Proofpoint or Rackspace
        else:
            black_list_domains.append(domain)
            return None
    except Exception as e:
        print(f"Error processing domain {email}: {e}")
        # check if email is not empty
        if domain != '':
            ignored_list_domains.append(domain)
        return None

# Read a list of domains from a CSV file
def read_rows_from_csv(file_path):
    rows = []
    with open(file_path, newline='', mode='r', encoding='utf-8', errors='replace') as csvfile:
        reader = csv.reader(csvfile)
        for index, row in enumerate(reader):
            #print(row)
            if index == 0:  # Skip the header row
                header_row.append(row)
                continue
            # if index > 3:  # Stop reading after the first three rows
                # break
            rows.append(row)  # Assuming the rows are in the first column
    return rows

# Write filtered domains to a new CSV file
def write_filtered_domains_to_csv(rows, file_path):
    # rows contains array of strings need to convert back to csv string

    with open(file_path, mode='w', newline='', encoding='utf-8', errors='replace') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(header_row)
        writer.writerows(rows)
        # for domain in domains:
        #     writer.writerow([domain])

# Batch processing function
def process_batch(rows, batch_size=100):
    filtered_rows = []
    
    # Split domains into batches
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        
        # Process each batch in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            future_to_row = {executor.submit(process_row, row): row for row in batch}
            for future in as_completed(future_to_row):
                result = future.result()
                if result:  # If domain is valid, add to the list
                    filtered_rows.append(result)

    return filtered_rows

if __name__ == "__main__":
    input_file = "email-list.csv"  # File with the list of email domains
    output_file = "filtered_email_list.csv"  # File to save filtered domains
    batch_size = 100  # Number of domains per batch

    # Read the domains from the input file
    rows = read_rows_from_csv(input_file)

    # Process domains in batches
    filtered_domains = process_batch(rows, batch_size=batch_size)

    # Write the filtered domains to the output file
    write_filtered_domains_to_csv(filtered_domains, output_file)

    # write the black list to a file
    with open('blacklist-domains.txt', 'w') as f:
        for domain in black_list_domains:
            f.write(domain + '\n')

    with open('ignored-domains.txt', 'w') as f:
        for domain in ignored_list_domains:
            f.write(domain + '\n')

    print(f"Filtered {len(filtered_domains)} domains that don't use Rackspace or Proofpoint.")
