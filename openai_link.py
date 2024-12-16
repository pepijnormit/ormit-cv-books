import openai
from openai import OpenAI
from datetime import datetime
from PyPDF2 import PdfWriter, PdfReader
from more_itertools import batched
import pandas as pd
from io import StringIO
import os
import shutil
import json 
import re
import time

def check_key(potential_key):
    try:
        client = OpenAI(
            api_key=potential_key,
        )
        
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": "Say this is a test",
                }
            ],
            model="gpt-4o-mini",
        )
        print('Key Correct')
        return True
    except openai.BadRequestError as e: # Don't forget to add openai
      # Handle error 400
        print(f"Error 400: {e}")
        return '400'
    except openai.AuthenticationError as e: # Don't forget to add openai
      # Handle error 401
        print(f"Error 401: {e}")
        return '401'
    except openai.PermissionDeniedError as e: # Don't forget to add openai
      # Handle error 403
        print(f"Error 403: {e}")
        return '403'
    except openai.NotFoundError as e: # Don't forget to add openai
      # Handle error 404
        print(f"Error 404: {e}")
        return '404'
    except openai.UnprocessableEntityError as e: # Don't forget to add openai
      # Handle error 422
        print(f"Error 422: {e}")
        return '422'
    except openai.RateLimitError as e: # Don't forget to add openai
      # Handle error 429
        print(f"Error 429: {e}")
        return '429'
    except openai.InternalServerError as e: # Don't forget to add openai
      # Handle error >=500
        print(f"Error >=500: {e}")
        return '500'
    except openai.APIConnectionError as e: # Don't forget to add openai
      # Handle API connection error
        print(f"API connection error: {e}")
        return 'API Connection Error'
    
def post_proc_str(data_str):
    data_clean = data_str.replace('`', '')
        #Sensical lines: contain @ (email address always succesful)
    filtered_text = "\n".join([line for line in data_clean.splitlines() if "@" in line])
    print(filtered_text)
    StringData = StringIO(filtered_text)
    cols_names = ['First Name', 'Last Name', 'Email', 'Phone Number', 'education level_BE', 'Afstudeerjaar (verwacht)', 'finish year', 'Faculty', 'Native language', 'pb_linkedin_profile_url']
    df = pd.read_csv(StringData, 
                     sep =",", 
                     names=cols_names,
                     index_col=False,
                     on_bad_lines='skip') #Careful here, could skip a lot
    return df

def categorize_faculty(column, fac_dic):
    new_column = []
    for item in column:
        # Convert to lowercase for case-insensitive matching if it's a string
        lower_item = item.lower() if isinstance(item, str) else ''  
        # If item is already a key in the dictionary, keep it as it is (in Title case)
        if lower_item in fac_dic.keys():
            new_column.append(lower_item.title())   
        # If item is found in the values of a key, replace it with the corresponding key (in Title case)
        elif any(lower_item in values for key, values in fac_dic.items()):
            key = next(key for key, values in fac_dic.items() if lower_item in values)
            new_column.append(key.title()) 
        # If neither found, append an empty string
        else:
            new_column.append('')
    
        # # Debug prints (can be removed or commented out)
        # print(item)
        # print(lower_item)
        # print(new_column)
    
    return new_column


def shift_row_if_contains_at(row):
    if '@' in row.iloc[1]:  # Check if the second column contains '@'
        # Shift the row to the right
        shifted_row = pd.Series([None] + row[:-1].tolist())
        shifted_row.index = row.index  # Align the indices

        # Split the new second column on spaces
        names = shifted_row[1].split(' ')
        
        if len(names) > 1:
            shifted_row.iloc[0] = names[0]  # First name goes to the first column
            shifted_row.iloc[1] = names[-1]  # Last name goes to the second column
        else:
            shifted_row.iloc[0] = shifted_row.iloc[1]  # If no space, move entire value to the first column
            shifted_row.iloc[1] = None  # Make the second column None if there's no space to split
        
        return shifted_row
    return row  # Return the row as is if no '@' in the second column

def txt_to_excel(dict_with_batchentries, file_path, cv_book_title='', jfws_title='', bo=True, dg=True):
    final = pd.DataFrame(columns=['First Name', 'Last Name', 'Email', 'Phone Number', 'education level_BE', 'Afstudeerjaar (verwacht)', 'finish year', 'Faculty', 'Native language', 'pb_linkedin_profile_url'])
    # for i in range(1,len(dict_with_batchentries)+1):
    #     result = post_proc_str(dict_with_batchentries[i])
    #     final = pd.concat([final, result])
    # final = final.reset_index(drop=True)
    
    #Approach 2: Check if the person isn't there yet
    # print(dict_with_batchentries)
    for i in range(1, len(dict_with_batchentries)+1):
        if i in dict_with_batchentries.keys(): #E.g. Timeout
            result = post_proc_str(dict_with_batchentries[i])
            
            # Filter out rows in `result` that are already present in `final`
            result_filtered = result[
                ~(((result['First Name'].isin(final['First Name'])) & 
                   (result['Last Name'].isin(final['Last Name']))) |
                  ((result['First Name'].isin(final['Last Name'])) & 
                   (result['Last Name'].isin(final['First Name'])))
                )
            ]
            
            final = pd.concat([final, result_filtered])
    final = final.reset_index(drop=True)
    df_cleaned = final #In case it remains empty
    print(final)
    
    #Notes 
    if not final.empty:
        # Sometimes name completely in First Name cell, fix this:
        df_shifted = final.apply(shift_row_if_contains_at, axis=1)
        
        # Strip from any spaces
        df_cleaned = df_shifted.map(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Delete rows where there are fewer than 3 entries:
        df_cleaned = df_cleaned[df_cleaned.apply(lambda x: x.count() >= 3, axis=1)]
            
        # Correct languages
        allowed_languages = ['French', 'Dutch', 'English']
        df_cleaned['Native language'] = df_cleaned['Native language'].apply(
            lambda x: x if x in allowed_languages else 'Other'
        )
        
        #Delete 【4:4†source】 like entries
        df_cleaned['pb_linkedin_profile_url'] = df_cleaned['pb_linkedin_profile_url'].apply(
            lambda x: re.sub(r'【.*?】', '', str(x)).strip()
        )
        
        #Delete N/A or n/a or Nan or nan or NAN entries
        df_cleaned = df_cleaned.apply(lambda col: col.map(lambda x: '' if str(x).lower() == 'n/a' else x))
        df_cleaned = df_cleaned.apply(lambda col: col.map(lambda x: '' if str(x).lower() == 'nan' else x))
    
        # Make names Sentence Case
        df_cleaned['First Name'] = df_cleaned['First Name'].str.title()
        df_cleaned['Last Name'] = df_cleaned['Last Name'].str.title()
        ##Extra check: Always 1 item as first name and avoid duplicates:
        df_cleaned['First Name'], df_cleaned['Last Name'] = zip(*df_cleaned.apply(
            lambda row: (
                row['First Name'].split()[0],  # Keep only the first word in 'First Name'
                row['Last Name'] if row['Last Name'].startswith(" ".join(row['First Name'].split()[1:])) else " ".join(row['First Name'].split()[1:]) + " " + row['Last Name']  # Adjust 'Last Name'
            ) if len(row['First Name'].split()) > 1 else (row['First Name'], row['Last Name']), axis=1))

        # Replace errors in diploma
        df_cleaned['education level_BE'] = df_cleaned['education level_BE'].apply(
        lambda x: str(x).replace("Master's", "Master").replace("Master Degree", "Master").strip()
        )
        
        df_cleaned['education level_BE'] = df_cleaned['education level_BE'].apply(
            lambda x: "Academic Bachelor" if x == "Bachelor" else x
            )
    
        # #Replace SYEARS (not found graduation years) with their respective degree duration
        # duration_degree = {
        #     'arts': 2,
        #     'bioscience engineering': 2,
        #     'economics & business': 1,
        #     'engineering science': 2,
        #     'engineering technology': 2,
        #     'law': 2,
        #     'management': 1,
        #     'medicine': 2,
        #     'social science': 2,
        #     'science': 2,
        #     'finance': 2
        # }
        # df_cleaned['Afstudeerjaar (verwacht)'] = df_cleaned.apply(
        #     lambda row: (
        #         (print(f"{row['First Name']} {row['Last Name']} - {row['Afstudeerjaar (verwacht)']} >> {str(int(row['Afstudeerjaar (verwacht)'][1:]) + duration_degree[row['Faculty'].lower()])}")) or 
        #         str(int(row['Afstudeerjaar (verwacht)'][1:]) + duration_degree[row['Faculty'].lower()])
        #     ) 
        #     if isinstance(row['Afstudeerjaar (verwacht)'], str) and row['Afstudeerjaar (verwacht)'].startswith('S') and row['Faculty'].lower() in duration_degree 
        #     else row['Afstudeerjaar (verwacht)'], 
        #     axis=1
        # )
        
        # Define the current year adjustment based on today's date: 
        # If End date and Start date found, and end date > start date, use end date
        # If no end date found (E0000), use adjusted_year: Assume this finishing academic year for everyone
        # If End date equal to/smaller than start date, use adjusted_year --> often same value when misunderstanding provided start date as not-provided end date.
        current_date = datetime.today()
        current_year = current_date.year

        if current_date.month >= 9:
            adjusted_year = current_year + 1 #Academic year finishes next year
        else:
            adjusted_year = current_year #Academic year finishes this year

        # Function to update the 'Afstudeerjaar (verwacht)' column based on 'finish year' and handle missing values
        def update_afstudeerjaar(row):
            # Check for missing values
            if pd.isna(row['Afstudeerjaar (verwacht)']) or row['Afstudeerjaar (verwacht)'] == '':
                # If 'Afstudeerjaar (verwacht)' is missing, return adjusted_year
                return str(adjusted_year)
            
            if pd.isna(row['finish year']) or row['finish year'] == '':
                # If 'finish year' is missing, return adjusted_year as well
                return str(adjusted_year)
            
            if isinstance(row['finish year'], str) and row['finish year'].startswith('E'):
                if row['finish year'] == 'E0000':
                    # For 'E0000', use the adjusted year logic
                    return str(adjusted_year)
                else:
                    finish_year = int(row['finish year'][1:])  # Extract the year from 'EYYYY'
                    expected_year = int(row['Afstudeerjaar (verwacht)'][1:])  # Extract the year from 'SYYYY'
                    
                    if finish_year > expected_year:
                        return str(finish_year)  # If EYYYY is larger than SYYYY, use EYYYY
                    else:
                        return str(adjusted_year)  # Otherwise, use adjusted_year
            else:
                return row['Afstudeerjaar (verwacht)']  # Keep original if not 'EYYYY'

        # Apply the update function
        df_cleaned['Afstudeerjaar (verwacht)'] = df_cleaned.apply(update_afstudeerjaar, axis=1)

        #Filter only valid years: Strings that will also be turned into digits, any empty spots will be filled by adjusted_year
        df_cleaned['Afstudeerjaar (verwacht)'] = df_cleaned['Afstudeerjaar (verwacht)'].apply(
            lambda x: int(x) if str(x).isdigit() else adjusted_year
        )

        #Supervise the faculties
        faculties = {
            'arts & philosophy': [
                'arts', 'humanities', 'history', 'philosophy', 
                'linguistics', 'literature', 'cultural studies', 'visual arts', 
                'musicology', 'theater studies', 'creative writing', 'art history'
            ],
            'economics & business': [
                'handelsingenieur', 'marketing', 'business engineering', 'business administration', 'economics', 
                'digital business engineering', 'sales management', 'accounting', 'applied economics',
                'entrepreneurship', 'finance', 'econometrics', 'supply chain management', 'global supply chain management', 
                'human resources', 'business analytics', 'international business', 'economics and management',
                'financial analysis and audit', 'banking & asset management', 
                'banking and asset management', 'banking', 'audit', 
                'financial analysis and audit', 'financial analysis & audit', 
                'auditing & finance', 'audit et analyse financière', 'banking and asset management', 
                'corporate finance', 'investment management', 'financial planning', 
                'risk management', 'financial accounting', 'taxation', 'banking and finance', 
                'insurance', 'financial engineering', 'auditing'
            ],
            'engineering & technology': [
                'information technology', 'software engineering', 'telecommunications', 
                'automation engineering', 'nanotechnology', 'biomedical technology', 
                'mechatronics', 'manufacturing engineering', 'construction engineering', 
                'power systems engineering', 'engineering', 'engineering science', 'applied science', 
                'business engineering & computer science', 'mechanical engineering', 
                'civil engineering', 'electrical engineering', 'computer science engineering', 
                'chemical engineering', 'aerospace engineering', 'industrial engineering', 
                'energy systems engineering', 'robotics', 'environmental engineering'
            ],
            'law & criminology': [
                'law and management', 'international law', 'criminal law', 
                'corporate law', 'constitutional law', 'intellectual property law', 
                'human rights law', 'environmental law', 'tax law', 
                'commercial law', 'labor law'
            ],
            'management': [
                'human resources management', 
                'law and management', 'management science', 'management sciences', 
                'business management', 'social and sustainable business management', 
                'intrapreneurship', 'social business management', 'project management', 
                'operations management', 'strategic management', 'supply chain management', 
                'management consulting', 'public administration', 'leadership studies', 
                'event management', 'hotel management', 'risk management'
            ],
            'health sciences': [
                'medicine', 'general medicine', 'nursing', 'pharmacy', 'dentistry', 
                'public health', 'surgery', 'psychiatry', 'pediatrics', 
                'radiology', 'physical therapy'
            ],
            'social sciences': [
                'social science', 'sociology', 'political science', 'anthropology', 
                'psychology', 'international relations', 'communication studies', 
                'social work', 'criminology', 'public policy', 'gender studies'
            ],
            'science': [
                'biomedical engineering', 'physics', 'chemistry', 'mathematics', 
                'biology', 'earth sciences', 'astronomy', 'biochemistry', 
                'geology', 'environmental science', 'microbiology', 'agricultural engineering', 'food science', 'environmental science', 
                'biotechnology', 'bioinformatics', 'sustainable energy engineering', 
                'plant sciences', 'water resource management', 'forest engineering', 
                'aquaculture and fisheries', 'bioscience engineering'
            ]
        }
    
        #Re-categorize first (save what's there to save)
        df_cleaned['Faculty'] = categorize_faculty(df_cleaned['Faculty'], faculties)
        df_cleaned['Faculty'] = df_cleaned['Faculty'].fillna('other')  # Replaces NaN values
        df_cleaned['Faculty'] = df_cleaned['Faculty'].replace('', 'other')  # Replaces empty strings
        df_cleaned['Faculty'] = df_cleaned['Faculty'].str.capitalize()
    
        #Remove any not website/links:
        df_cleaned['pb_linkedin_profile_url'] = df_cleaned['pb_linkedin_profile_url'].apply(
            lambda x: x if x.startswith(('www', 'http')) else ''
        )
        #Add columns
        df_cleaned['R&S |CR|CV book'] = cv_book_title
        df_cleaned['R&S |CR|JF & WS'] = jfws_title
        if bo:
            df_cleaned['Bedrijfsonderdeel intern'] = 'Ormit Talent België'
        if dg:
            df_cleaned['Doelgroep'] ='B2C'
    
    # path, extension = os.path.splitext(file_path)
    # df_cleaned.to_excel(path + f'_organized_attempt{attempt}.xlsx', index=False)
    df_cleaned_final = df_cleaned.drop(columns='finish year')
    return df_cleaned_final