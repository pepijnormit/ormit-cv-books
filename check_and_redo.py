import os
import pandas as pd
from unidecode import unidecode
import shutil


def check_df(cv_folder, source_cvs, df): 
    pdf_filenames = [f for f in os.listdir(cv_folder) if f.endswith('.pdf')]

    # Initialize a set to keep track of unique names
    unique_names = set()
    rows_to_keep = []

    for index, row in df.iterrows():
        first_name = unidecode(row['First Name'].strip()).lower()
        last_name = unidecode(row['Last Name'].strip()).lower()

        # Create keys for both (first_name, last_name) and (last_name, first_name)
        normal_key = (first_name, last_name)
        reverse_key = (last_name, first_name)

        # Check if either key is already in the set
        if normal_key not in unique_names and reverse_key not in unique_names:
            unique_names.add(normal_key)  # Add the normal key
            rows_to_keep.append(index)     # Keep the index of the first occurrence

    # Create a new DataFrame with only the unique names
    df_unique = df.loc[rows_to_keep].reset_index(drop=True)

    # Check duplicates
    name_dict = {}
    for index, row in df_unique.iterrows():
        first_name = unidecode(row['First Name'].strip()).lower().split()[0]
        last_name = unidecode(row['Last Name'].strip()).lower()
        full_name = f"{row['First Name'].strip()} {row['Last Name'].strip()}"

        # Define both (first_name, last_name) and (last_name, first_name) as possible keys
        key_normal = (first_name, last_name)
        key_reversed = (last_name, first_name)
        
        # Check if either key exists in the dictionary
        if key_normal in name_dict:
            name_dict[key_normal].append(full_name)
        elif key_reversed in name_dict:
            name_dict[key_reversed].append(full_name)
        else:
            name_dict[key_normal] = [full_name]

    # Check the number of unique keys and print duplicates
    print(f"Total unique (first_name, last_name) keys: {len(name_dict)} / {len(df)}")
    duplicates = {k: v for k, v in name_dict.items() if len(v) > 1}
    print("Duplicate (first_name, last_name) entries in name_dict:")
    for key, names in duplicates.items():
        print(f"{key}: {names}")
    name_dict_len = len(name_dict)

    # Create lists to store matched results and track unmatched names and files
    matched_results = []
    unmatched_files = pdf_filenames[:]  # Start with all files; remove matched ones later
    unmatched_names = list(name_dict.keys())  # Track unmatched names by key for best match checking

    # First Pass: Check for exact "best matches" (both first and last name in filename, in any order)
    for filename in pdf_filenames:
        normalized_filename = unidecode(filename).lower()
        
        # Look for filenames that contain both first and last names, regardless of order
        exact_matches = [
            name for name in name_dict 
            if name[0] in normalized_filename and name[1] in normalized_filename
        ]
        
        if len(exact_matches) == 1:
            # Unique exact match found
            match = exact_matches[0]
            matched_results.append((name_dict[match], filename))
            unmatched_files.remove(filename)  # Remove matched file
            unmatched_names.remove(match)  # Remove matched name by key
            del name_dict[match]  # Remove the matched name from name_dict to avoid reuse

        # elif len(exact_matches) > 1:
            # print(f"Multiple exact matches for {filename}. No unique match established.")
            
    # Second Pass: Check for partial matches (first name or last name only) for remaining unmatched files
    for filename in unmatched_files[:]:  # Iterate over a copy of unmatched_files
        normalized_filename = unidecode(filename).lower()
        
        # Check for first name match only
        matched_names = [name for name in unmatched_names if name[0] in normalized_filename]
        
        if len(matched_names) == 1:
            # Single first name match found
            match = matched_names[0]
            matched_results.append((name_dict[match], filename))
            unmatched_files.remove(filename)  # Remove matched file
            unmatched_names.remove(match)  # Remove matched name
            del name_dict[match]  # Remove from name_dict

        elif len(matched_names) > 1:
            # Multiple matches - refine with last names
            refined_matches = []
            for first_name, last_name in matched_names:
                last_name_parts = last_name.split()
                last_name_to_check = last_name_parts[-1] if len(last_name_parts) > 1 else last_name_parts[0]

                if last_name_to_check in normalized_filename:
                    refined_matches.append((first_name, last_name))
            
            if len(refined_matches) == 1:
                # Single refined match found
                match = refined_matches[0]
                matched_results.append((name_dict[match], filename))
                unmatched_files.remove(filename)  # Remove matched file
                unmatched_names.remove(match)  # Remove matched name
                del name_dict[match]  # Remove from name_dict
            # else:
                # print(f"Multiple refined matches or no match for {filename}")

        else:
            # No first name match found; check for last name matches only
            last_name_matches = [name for name in unmatched_names if name[1] in normalized_filename]
            
            if len(last_name_matches) == 1:
                # Single last name match found
                match = last_name_matches[0]
                matched_results.append((name_dict[match], filename))
                unmatched_files.remove(filename)  # Remove matched file
                unmatched_names.remove(match)  # Remove matched name
                # print(f"Matched on last name only: {name_dict[match]} with {filename}")
                del name_dict[match]  # Remove from name_dict

            # elif len(last_name_matches) > 1:
                # Multiple matches found with the last name
                # print(f"Multiple last name matches for {filename}. No unique match established.")
            # else:
                # print(f"No match for {filename} - first and last name not found")

    # Display matched results as a DataFrame
    matched_df = pd.DataFrame(matched_results, columns=["Full Name", "Filename"])

    # Display unmatched filenames and unmatched names as DataFrames
    unmatched_files_df = pd.DataFrame(unmatched_files, columns=["Unmatched Filename"])
    unmatched_names_df = pd.DataFrame([name_dict[name] for name in unmatched_names], columns=["Unmatched Name"])

    # Summary of results
    print(f'\nSummary:')
    print(f'Number of files checked: {len(pdf_filenames)}')
    print(f'Number of unique names in DF: {name_dict_len}')
    print(f'Matches: {len(matched_df)}')
    print(f'Unmatched Names: {len(unmatched_names_df)}')
    print(f'Unmatched Filenames: {len(unmatched_files_df)}')
    print(unmatched_files_df)


    # Move unmatched files to 'not_matched' folder
    parent_dir = os.path.dirname(cv_folder) 
    unmatched_folder = os.path.join(source_cvs, 'UNDONE').replace("\\", "/")
    # Create 'not_matched' folder if it doesn't exist, otherwise clear it
    if not os.path.exists(unmatched_folder):
        os.makedirs(unmatched_folder)
    else:
        # Empty the folder if it already exists
        for file in os.listdir(unmatched_folder):
            file_path = os.path.join(unmatched_folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                
    if len(unmatched_files) != 0:            
        for filename in unmatched_files:  # `unmatched_files is your list of unmatched filenames
            source_path = os.path.join(source_cvs, filename).replace("\\", "/")
            destination_path = os.path.join(unmatched_folder, filename).replace("\\", "/")
            shutil.copy2(source_path, destination_path)  # Copy file with metadata

    ###Delete rows of untracable entries 
    unmatched_names_set = set(unmatched_names)  # Convert unmatched names to a set for fast lookup
    df_final = df_unique[~df_unique.apply(lambda row: (unidecode(row['First Name'].strip()).lower().split()[0], 
                                   unidecode(row['Last Name'].strip()).lower()) in unmatched_names_set, axis=1)]

    # Create a Full Name column in the main DataFrame by combining First Name and Last Name
    df_final['Full Name'] = df_final['First Name'].str.strip() + ' ' + df_final['Last Name'].str.strip()

    # Convert matched_results to a DataFrame with Full Name and Filename
    matched_df = pd.DataFrame(matched_results, columns=["Full Name", "CV Filename"])
    matched_df['Full Name'] = matched_df['Full Name'].apply(lambda x: x[0] if x else '')
    # Merge the matched filenames into the main DataFrame based on Full Name
    df_final = pd.merge(df_final, matched_df[['Full Name', 'CV Filename']], on='Full Name', how='left')

    # Drop the 'Full Name' column if it's no longer needed
    df_final = df_final.drop(columns=['Full Name'])


    return df_final, unmatched_folder
# clean_df, folder_undone = check_df(fol_loc, df)