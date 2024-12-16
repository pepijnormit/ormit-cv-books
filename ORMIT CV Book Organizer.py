#GUI
##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
import sys
import shutil
from shutil import rmtree
import json
import os
from time import sleep, time
from PyQt6.QtCore import Qt #, QTimer
from PyQt6.QtWidgets import QApplication, QWidget, QPushButton, QLineEdit, QLabel, QGridLayout, QFileDialog, QCheckBox, QComboBox, QVBoxLayout, QProgressBar #, QListWidget
from PyQt6.QtGui import QPixmap, QFont, QIntValidator, QIcon #, QScreen
from pathlib import Path
## Processing
from datetime import datetime
from PyPDF2 import PdfWriter, PdfReader
from more_itertools import batched
from openai_link import *
from check_and_redo import *
from PyQt6.QtCore import QThread, pyqtSignal
import signal
import psutil

def terminate_subprocesses():
    parent_pid = os.getpid()
    parent = psutil.Process(parent_pid)
    for child in parent.children(recursive=True):
        os.kill(child.pid, signal.SIGTERM)
    
time_limit = 120

### -------------------------------------------------------------------------
logo_path_abs = "resources/ormittalentV3.png"
icon_path_abs = "resources/IconV2.ico"
key_file_path_abs = "resources/saved_key.txt"

cv_books = ['', 'CV Book AMS', 'CV Book Ekonomika','CV Book HEC Liège',
            'CV Book Inisol', 'CV Book KEPS', 'CV Book LSM', 'CV Book UA',
            'CV Book UCL Mons', 'CV Book VEK', 'CV Book ICHEC','CV Book AFC Leuven',
            'CV Book AFC Gent','CV Book ABSOC','CV Book UHasselt','CV Book Vlerick',
            'CV Book Solvay','CV Book UGent','CV Book UCL Mons','CV Book AFD',
            'CV Book Groep T','CV Book Jobhappen Kortrijk']

jfws_list = ['', 'BG Ekonomika', 'Enactus', 'JF ABSOC', 'JF AMS', 'JF Ekonomika',
            'JF Ekonomika Kiesweek', 'JF HEC', 'JF HEC Liège', 'JF ICHEC',
            'JF Inisol', 'JF IT Ekonomika', 'JF KUL', 'JF LSM', 'JF Solvay',
            'JF UAntwerpen', 'JF UGent', 'JF UHasselt', 'JF VEK', 'JF Vlerick',
            'JF VUB', 'JF Kortrijk', 'JF UCL Mons', 'JF UHasselt', 'Unamur Career center',
            'WS AFC Gent', 'WS AFC Leuven', 'WS AMS', 'WS Ekonomika', 'WS HEC Liège',
            'WS ICHEC', 'WS UAntwerpen', 'WS UGent', 'WS VEK', 'WS NHiTec',
            'WS Le Wagon', 'WS Junior Consulting Louvain','WS LSM']

master_prompt = """Summarize the following personal info per file, and give me the output collectively with one line per person, with the following personal info all seperated by comma without index:
   1. First Name
   2. Last Name
   3. Email address
   4. Phone number
   5. Education level of the ongoing degree. First look for a finished or ongoing Master's degree: Return 'Master' if that's the case. Only if no information on Master's degree is found, return the highest degree done or currently being done by this person. Choose STRICTLY from the only options Master, Academic Bachelor, Professional Bachelor or Secondary level. The difference between Academic and Professional bachelor is that Academic Bachelor is taught at university, and any other (non-university) Bachelor is a Professional Bachelor.
   6. Firstly list the educational background of this person in chronological order. Look at the most recent, ongoing program of this chronological list and return the starting year of this program preceded by the letter 'S', e.g. S2023 if they started the most recent program in 2023.
   7. Firstly list the educational background of this person in chronological order. Look at the ongoing program of this chronological list and if you find a numerical expected graduation year of this program, return it preceded by the letter 'E', e.g. E2025 if they started the most recent program in 2023 and explicitly mention the expected graduation date of 2025. If you find none (or only 'Now' or 'Present', return E0000).
   8. Faculty of the ongoing degree. If there is no ongoing degree today, return the faculty of their last finished degree, Choose strictly from the only options Arts & Philosophy, Economics & business, Management, Engineering & Technology, Social Sciences, Law & Criminology, Science, Health Sciences. You should categorize any other faculties under one of these labels. If the degree is 'Business Engineering' or 'Handelsingenieur' or 'Ingénieur de Gestion' it MUST be categorized as 'Economics & business'. If there is anything with 'Management' in the degree title, it MUST be faculty Management*. 
   9. Native language (choose from the four options 'Dutch', 'French', 'English' or the label 'Other'. Do not allow any other languages. If they are mulitlangual and one of the languages is French/Dutch/English, those labels are preferred over the label 'Other')
   10. A link to one of the following services if provided: Wefynd, Karamel, LinkedIn, Shortlist, or a personal website
   If any of the info is missing, leave a blank space. Be concise without any conversational lines besides the explained format per person:
       first name, last name, email address, phone number, highest education level, starting year, ending year, faculty, native language, link to one of the services (if applicable)"""
  
#6. Firstly list all the educational experiences of this person in chronological order. Go over this list and collect the starting year of that most recent program preceded by the letter 'S' (e.g. S2022 if they started that current degree in 2022).

# Organize alphabetically & with custom 'other' option
for lst in [cv_books, jfws_list]:
    lst.sort()

# Function to get the correct path for accessing resources in a bundled app
def resource_path(relative_path):
    """ Get the absolute path to a resource, works for PyInstaller and development environments """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Use resource_path() to access the saved_key.txt file
key_file_path = resource_path(key_file_path_abs)
logo_path = resource_path(logo_path_abs)
icon_path = resource_path(icon_path_abs)

### ----------------------------  Progress bar ------------------------------
class ProgressBar(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Converting CV book')
        self.setWindowIcon(QIcon(icon_path))  # Use .ico for Windows

        self.resize(600, 80)
        
        # Center the window
        self.center()

        layout = QVBoxLayout()

        # Create and configure the progress bar
        self.progress = QProgressBar(self)
        self.progress.setValue(0)
        layout.addWidget(self.progress)

        # Add label for feedback
        self.label = QLabel("Progress: 0%", self)
        layout.addWidget(self.label)

        self.setLayout(layout)

    # Function to center the window on screen
    def center(self):
        screen = QApplication.primaryScreen()  # Get the primary screen
        screen_geometry = screen.availableGeometry()
        window_geometry = self.frameGeometry()
        window_geometry.moveCenter(screen_geometry.center())
        self.move(window_geometry.topLeft())
    
    # Function to update the progress bar
    def update_progress(self, percentage, file_progress=-10, desc='File being processed'):
        self.progress.setValue(percentage)  # Set progress to the specified percentage
        if file_progress > -1:
            self.label.setText(f"{desc} {file_progress:.0f}%")  # Update label
        else:
            self.label.setText(f"{desc}")  # Update label with text only     
        QApplication.processEvents()

### ----------------------------  Menu  ----------------------------------
class MainWindow(QWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.progress_window = ProgressBar()  # Initialize ProgressBar
        self.setWindowTitle("ORMIT - Organize CV book v1.0")
        self.setWindowIcon(QIcon(icon_path))  # Use .ico for Windows
        #Put on top and show:
        self.setWindowFlag(Qt.WindowType.WindowStaysOnTopHint)
        self.activateWindow()  # Activates the window
        self.raise_()          # Raises the window to the top
        self.setFocus()        # Sets keyboard focus to the window
        
        self.setStyleSheet("background-color: white; color: black;") 
        bold_font = QFont()
        bold_font.setBold(True)

        # self.setFixedHeight(240)
        self.setFixedWidth(1000)
        
        # set the grid layout
        layout = QGridLayout()
        self.setLayout(layout)
        
        # Load the logo
        pixmap = QPixmap(logo_path)
        pixmap_label = QLabel()
        pixmap_label.setScaledContents(True)
        resize_fac=4
        scaled_pixmap = pixmap.scaled(
            round(pixmap.width() / resize_fac),
            round(pixmap.height() / resize_fac),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation)  
        pixmap_label.setPixmap(scaled_pixmap)
        layout.addWidget(pixmap_label, 0, 0, 1, 2)
        
        # Subtitle 1
        self.key_label = QLabel('File & Key selection')
        self.key_label.setFont(bold_font)
        layout.addWidget(self.key_label, 1, 0, 1, 2)

        # Key input label
        self.key_label = QLabel('OpenAI key:')
        self.key_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.key_label, 2, 0, 1, 1)

        # QLineEdit for OpenAI key
        self.key_insert = QLineEdit(placeholderText='sk-******')
        self.key_insert.textChanged.connect(self.store_key)
        layout.addWidget(self.key_insert, 2, 1, 1, 3)

        # Load the key from the file when the app starts
        self.load_key()

        # Delete key button
        self.delete_key_btn = QPushButton('Delete Key')
        self.delete_key_btn.setFixedWidth(90)
        self.delete_key_btn.clicked.connect(self.delete_key)
        layout.addWidget(self.delete_key_btn, 3, 0)        
        
        # browse for file https://www.pythontutorial.net/pyqt/pyqt-qfiledialog/
        self.file_browser_btn = QPushButton('Browse')
        self.file_browser_btn.setFixedWidth(70)
        self.file_browser_btn.clicked.connect(self.open_folder_dialog)
        self.file_list = QLabel(self)

        layout.addWidget(self.file_browser_btn, 5 ,0)
        layout.addWidget(self.file_list, 5, 1, 1, 3)
        self.show()
               
        # categories
        self.cat_label = QLabel('Categories')
        self.cat_label.setFont(bold_font)
        self.cat_label.setAlignment(Qt.AlignmentFlag.AlignBottom)
        layout.addWidget(self.cat_label, 1, 4)
        
        global bo_final
        bo_final = True #Default
        self.b1 = QCheckBox("Include bedrijfsonderdeel")
        self.b1.setChecked(True) #Default
        self.b1.toggled.connect(lambda:self.btnstate_bo(self.b1))
        layout.addWidget(self.b1, 2,4) #layout.addWidget(widget, row, column, rowSpan, columnSpan, alignment)
 
        global dg_final
        dg_final = True #Default
        self.b2 = QCheckBox("Include doelgroep")
        self.b2.setChecked(True) #Default
        self.b2.toggled.connect(lambda:self.btnstate_dg(self.b2))
        layout.addWidget(self.b2, 3,4) #layout.addWidget(widget, row, column, rowSpan, columnSpan, alignment)

        #comboboxes
        cat_label = QLabel('CV Book Title:')
        layout.addWidget(cat_label, 4, 4)
        
        self.combo_title = QComboBox(self)
        for i in cv_books:
                self.combo_title.addItem(i)     
        global cv_book_final
        cv_book_final= ''
        self.combo_title.currentIndexChanged.connect(lambda:self.selectionchange_cvtitle(self.combo_title)) #If selected from options
        self.combo_title.currentTextChanged.connect(lambda:self.custom_cvtitle(self.combo_title)) #If customly entered
        self.combo_title.setToolTip('Select a CV book title')
        self.combo_title.setEditable(True) 
        # self.combo_title.textChanged.connect(self.custom_cvtitle) #update whenever changed (no 'enter' required)
        layout.addWidget(self.combo_title, 5,4)
       
        cat_label = QLabel('CV Book Source:')
        layout.addWidget(cat_label, 6, 4)
        
        self.combo_source = QComboBox(self)
        for i in jfws_list:
                self.combo_source.addItem(i)  
        global jfws_final
        jfws_final = ''
        self.combo_source.currentIndexChanged.connect(lambda:self.selectionchange_source(self.combo_source))
        self.combo_source.currentTextChanged.connect(lambda:self.custom_source(self.combo_source)) #If customly entered
        self.combo_source.setToolTip('Select a CV book source')
        self.combo_source.setEditable(True) 
        layout.addWidget(self.combo_source, 7,4)
        
        # skip first page(s)
        self.b3 = QCheckBox("Skip first page(s)")
        global skip_first
        skip_first = 0
        self.b3.toggled.connect(lambda:self.btnstate_xtra(self.b3))
        layout.addWidget(self.b3, 8,4) #layout.addWidget(widget, row, column, rowSpan, columnSpan, alignment)

        self.firstpages = QLineEdit()
        onlyInt = QIntValidator()
        onlyInt.setRange(0, 9)
        self.firstpages.setValidator(onlyInt)
        self.firstpages.setFixedWidth(40)
        self.firstpages.setPlaceholderText("1")
        self.firstpages.hide()
        self.firstpages.textChanged.connect(self.print_pages) #update whenever changed (no 'enter' required)
        layout.addWidget(self.firstpages, 9, 4)
        
        # Submit
        self.submitbtn = QPushButton('Submit')
        self.submitbtn.setFixedWidth(90)
        self.submitbtn.clicked.connect(self.submit)
        self.submitbtn.hide()
        layout.addWidget(self.submitbtn, 10,4)
      
        # In the MainWindow class, add a checkbox for '1 FILE'
        self.one_file_check = QCheckBox("1 FILE")
        self.one_file_check.setChecked(False)  # Default unchecked
        self.one_file_check.toggled.connect(self.toggle_file_mode)
        layout.addWidget(self.one_file_check, 0, 4)  # Adjust positioning as needed
        
        # Add a 'Browse for 1 File' button, initially hidden
        self.file_browser_single_btn = QPushButton('Browse')
        self.file_browser_single_btn.setFixedWidth(70)
        self.file_browser_single_btn.clicked.connect(self.open_file_dialog)
        self.file_browser_single_btn.hide()  # Initially hidden
        layout.addWidget(self.file_browser_single_btn, 5, 0)

    # Modify the `open_folder_dialog` and `submit` methods
    def toggle_file_mode(self, checked):
        """Toggle between folder mode and single file mode."""
        if checked:
            self.file_browser_btn.hide()  # Hide folder browse button
            self.file_browser_single_btn.show()  # Show single file browse button
        else:
            self.file_browser_single_btn.hide()  # Hide single file browse button
            self.file_browser_btn.show()  # Show folder browse button
    
    def open_file_dialog(self):
        """Open a dialog to select a single PDF file."""
        dialog = QFileDialog(self)
        dialog.setFileMode(QFileDialog.FileMode.ExistingFile)
        file_path, _ = QFileDialog.getOpenFileName(self, 'Select File', '', 'PDF Files (*.pdf)')
        if file_path:
            print(f"Selected file: {file_path}")
            global single_file
            single_file = file_path
            self.file_list.setText(str(single_file))
            self.submitbtn.show()  # Show submit button only after selecting a file     

    def store_key(self, key):
        """Store the OpenAI key to a file."""
        with open(key_file_path, "w") as key_file:
            key_file.write(key)

    def load_key(self):
        """Load the OpenAI key from a file if it exists."""
        if os.path.exists(key_file_path):
            with open(key_file_path, "r") as key_file:
                saved_key = key_file.read().strip()
                self.key_insert.setText(saved_key)  # Set the saved key into the QLineEdit field
        else:
            print("No key saved previously.")

    def delete_key(self):
        """Delete the saved key file."""
        if os.path.exists(key_file_path):
            os.remove(key_file_path)
            self.key_insert.clear()  # Clear the QLineEdit field
            print("Key has been deleted.")
        else:
            print("No key found to delete.")
        
    def btnstate_bo(self,b):
        global bo_final
        if b.isChecked() == True:
           print(b.text()+" selected")
           bo_final = True
        else:
           print(b.text()+" deselected")
           bo_final = False
    
    def btnstate_dg(self,b):
        global dg_final
        if b.isChecked() == True:
           print(b.text()+" selected")
           dg_final = True
        else:
           print(b.text()+" deselected")
           dg_final = False
           
    def btnstate_xtra(self,b):
        if b.isChecked() == True:
            print(b.text()+" selected")
            self.firstpages.show()
            self.firstpages.setFocus()
        else:
            print(b.text()+" deselected")
            self.firstpages.hide()
            first_pages = 0

    def selectionchange_cvtitle(self,i):	
        global cv_book_final
        cv_book_final = i.currentText()
        print("Selection changed: ",i.currentText())
    
    def custom_cvtitle(self, i):
        global cv_book_final
        cv_book_final = i.currentText()
        # print(i.currentText())

    def selectionchange_source(self,i):	
        global jfws_final
        jfws_final = i.currentText()
        print("Selection changed: ",i.currentText())

    def custom_source(self, i):
        global jfws_final
        jfws_final = i.currentText()
        # print(i.currentText())
    
    def print_pages(self, k):
        global skip_first
        if k == "":
            skip_first = 0
        else:
            skip_first = int(k)
        print(skip_first) 
   
    def open_folder_dialog(self):
        dialog = QFileDialog(self)
        dialog.setFileMode(QFileDialog.FileMode.Directory)
        folder_path = QFileDialog.getExistingDirectory(self, 'Select Folder')
        if folder_path:
            print(f"Selected folder: {folder_path}")
            global folder
            folder = folder_path
            self.submitbtn.show()  # Show submit button only after selecting a folder
            self.file_list.setText(str(folder_path))


    def process_multiple_files(self, s, sf=False): #sf: If come from single file, no need to match to filename
        key_openai = self.key_insert.text()
        if (len(key_openai) < 20) or key_openai[0:3] != 'sk-':  # For sure no valid key
            print('Unsuccessfully submitted')
            self.key_insert.setStyleSheet("color: red;")
        else:
            print('Successfully submitted')
    
            self.close()  # Close the window
    
            global gui_data
            gui_data = {'key': key_openai,
                        'folder': folder,
                        'cvbook': cv_book_final,
                        'jfws': jfws_final,
                        'bo': bo_final,
                        'dg': dg_final,
                        'skip_first_p': skip_first
                        }
    
            # print(gui_data)
    
            # Open prog_bar
            prog_bar = ProgressBar()
            prog_bar.show()
            prog_bar.update_progress(0, desc='Testing key...')  

            start = time.time()
            # Check Key first
            validkey = check_key(gui_data['key'])
            if validkey != True:
                error_message = f"ERROR: {validkey} detected"
                print(error_message)
            else:
                # Continue with processing multiple PDFs in a folder
                pdf_files = [f for f in os.listdir(gui_data['folder']) if f.endswith('.pdf')]
                npdfs = len(pdf_files)
                # print(f"Found {npdfs} PDFs in the selected folder.")
    
                batchsize = 4 #5
                batch_count = 1
                results = {}
                
                prog_bar.update_progress(0, desc=f"Key correct - Found {npdfs} PDFs: Connecting to OpenAI...")  
                
                client = OpenAI(api_key=gui_data['key'])                
                assistant = client.beta.assistants.create(
                  name="ORMIT CV Organizer",
                  instructions="You are reading CVs and summarizing the personal details from the files. Use you knowledge base to organize the CVs based on the request by the user",
                  model="gpt-4o-mini",
                  tools=[{"type": "file_search"}],
                )
                # Process each batch of PDFs
                # for batch in batched(pdf_files, batchsize):
                total_batches = len(pdf_files) // batchsize + (1 if len(pdf_files) % batchsize > 0 else 0)
                for i, batch in enumerate(batched(pdf_files, batchsize)):
                    batch_starttime = time.time()
    
                    folder_stamp = datetime.now().strftime("temp_file%d%m%Y%H%M")
                    os.mkdir(folder_stamp)
    
                    lst_files = []
                    for pdf_file in batch:
                        shutil.copy(os.path.join(gui_data['folder'], pdf_file), f"{folder_stamp}/{pdf_file}")
                        lst_files.append(os.path.join(folder_stamp, pdf_file))
    
                    # Create vector store
                    if batch_count == 1:
                        vector_store = client.beta.vector_stores.create(name="CVs", expires_after={
                            "anchor": "last_active_at",
                            "days": 1
                        })
    
                    # Upload files and send to OpenAI
                    file_streams = [open(path, "rb") for path in lst_files]
                    file_batch = client.beta.vector_stores.file_batches.upload_and_poll(
                        vector_store_id=vector_store.id, files=file_streams
                    )
    
                    for file in file_streams:
                        file.close()
                    
                    percentage = round(((i + 1) / total_batches) * 80) #Keep 20 percent for final processing
                    prog_bar.update_progress(int(percentage), desc=f"Organizing CVs batch: {i+1}/{total_batches}")  
                    
                    #(Re)connect assistant to vector store(s)
                    assistant = client.beta.assistants.update(
                      assistant_id=assistant.id,
                      tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}},
                    )
                            
                    assistID = assistant.id
                        
                    # client = OpenAI()
                    empty_thread = client.beta.threads.create()

                    client.beta.threads.messages.create(
                           empty_thread.id,
                           role="user",
                           content= master_prompt,
                            )
                    run = client.beta.threads.runs.create(
                        thread_id=empty_thread.id,
                        assistant_id=assistant.id,
                    )
    
                    while run.status != 'completed':
                        # Check if the time limit has been exceeded
                        elapsed_time = time.time() - batch_starttime
                        if elapsed_time > time_limit:
                            print("Time limit exceeded, exiting the loop.")
                            break  # Or raise an exception, or handle it as needed
                    
                        run = client.beta.threads.runs.retrieve(
                            thread_id=empty_thread.id,
                            run_id=run.id
                        )
                    
                    if run.status == 'completed':
                        output = client.beta.threads.messages.list(thread_id=empty_thread.id, run_id=run.id)
                    
                        messages = list(output)
                        
                        message_content = messages[0].content[0].text
                        test = message_content.value
                        print(test)
                        results[batch_count] = test
                    
                        endopenai = time.time()
                        total_time = endopenai - batch_starttime
                        
                        # First, create a set of the current file IDs to track what to delete
                        file_ids = {file.id for file in client.files.list()}
                        print(file_ids)
                        
                        # Then attempt deletion only for files that are still in the list
                        for file_id in file_ids:
                            client.files.delete(file_id)
                            print(f"Deleted file {file_id}")
                                
                        enddeleting = time.time()
                        total_time = enddeleting - endopenai
                        # print(f"\n Deleting time: {total_time:.2f}")
       
                    shutil.rmtree(folder_stamp)
                    batch_count += 1
                
                client.beta.assistants.delete(assistant.id)
                df_first_attempt = txt_to_excel(results, gui_data['folder'], cv_book_title=cv_book_final, jfws_title=jfws_final, bo=bo_final, dg=dg_final)
                
                ###ACCOUNTABILITY: Check uniqueness and file tracability of all people in df:
                if not sf:
                    clean_df, folder_undone = check_df(gui_data['folder'], gui_data['folder'], df_first_attempt)
                else:
                    clean_df = df_first_attempt
                clean_df.to_excel(gui_data['folder'] + f'/organized_attempt1.xlsx', index=False)
    
                ### Second attempt: Redo those that couldn't be found in the resulting df (or traced back to a cv)
                if not sf:
                    results2 = {}
                    pdf_files = [f for f in os.listdir(folder_undone) if f.endswith('.pdf')]
                    npdfs = len(pdf_files)
                    print(f"Found {npdfs} PDFs in the UNDONE folder.")
        
                    prog_bar.update_progress(0, desc=f"Second run: Found {npdfs} CVs for second attempt...")  

                    batch_count = 1
                    results = {}
        
                    client = OpenAI(api_key=gui_data['key'])                
                    assistant = client.beta.assistants.create(
                      name="ORMIT CV Organizer",
                      instructions="You are reading CVs and summarizing the personal details from the files. Use you knowledge base to organize the CVs based on the request by the user",
                      model="gpt-4o-mini",
                      tools=[{"type": "file_search"}],
                    )
                    # Process each batch of PDFs
                    total_batches = len(pdf_files) // batchsize + (1 if len(pdf_files) % batchsize > 0 else 0)
                    for i, batch in enumerate(batched(pdf_files, batchsize)):
                    # for batch in batched(pdf_files, batchsize):
                        batch_starttime = time.time()
        
                        folder_stamp = datetime.now().strftime("temp_file%d%m%Y%H%M")
                        os.mkdir(folder_stamp)
        
                        lst_files = []
                        for pdf_file in batch:
                            shutil.copy(os.path.join(folder_undone, pdf_file), f"{folder_stamp}/{pdf_file}")
                            lst_files.append(os.path.join(folder_stamp, pdf_file))
        
                        # Create vector store
                        if batch_count == 1:
                            vector_store = client.beta.vector_stores.create(name="CVs", expires_after={
                                "anchor": "last_active_at",
                                "days": 1
                            })
        
                        # Upload files and send to OpenAI
                        file_streams = [open(path, "rb") for path in lst_files]
                        file_batch = client.beta.vector_stores.file_batches.upload_and_poll(
                            vector_store_id=vector_store.id, files=file_streams
                        )
        
                        for file in file_streams:
                            file.close()
                        
                        percentage = round(((i + 1) / total_batches) * 80) #Keep 20 percent for final processing
                        prog_bar.update_progress(int(percentage), desc=f"Second run - Organizing CVs batch: {i+1}/{total_batches}")  

                        
                        #(Re)connect assistant to vector store(s)
                        assistant = client.beta.assistants.update(
                          assistant_id=assistant.id,
                          tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}},
                        )
                                
                        assistID = assistant.id
                            
                        client = OpenAI()
                        empty_thread = client.beta.threads.create()
        
                        client.beta.threads.messages.create(
                               empty_thread.id,
                               role="user",
                               content=master_prompt
                                )
                        run = client.beta.threads.runs.create(
                            thread_id=empty_thread.id,
                            assistant_id=assistant.id,
                        )
        
                        while run.status != 'completed':
                            # Check if the time limit has been exceeded
                            elapsed_time = time.time() - batch_starttime
                            if elapsed_time > time_limit:
                                print("Time limit exceeded, exiting the loop.")
                                break  # Or raise an exception, or handle it as needed
                        
                            run = client.beta.threads.runs.retrieve(
                                thread_id=empty_thread.id,
                                run_id=run.id
                            )
                        
                        if run.status == 'completed':
                            output = client.beta.threads.messages.list(thread_id=empty_thread.id, run_id=run.id)
                        
                            messages = list(output)
                            
                            message_content = messages[0].content[0].text
                            test = message_content.value
                            print(test)
                            results2[batch_count] = test
                        
                            endopenai = time.time()
                            total_time = endopenai - batch_starttime
                            
                            # Clean vector store
                            file_ids = {file.id for file in client.files.list()}
                            print(file_ids)
                            
                            # Then attempt deletion only for files that are still in the list
                            for file_id in file_ids:
                                client.files.delete(file_id)
                                print(f"Deleted file {file_id}")
                            
                            enddeleting = time.time()
                            total_time = enddeleting - endopenai
           
                        shutil.rmtree(folder_stamp)
                        batch_count += 1   
                        
                    client.beta.assistants.delete(assistant.id)
                    
                    df_second_attempt = txt_to_excel(results2, folder_undone, cv_book_title=cv_book_final, jfws_title=jfws_final, bo=bo_final, dg=dg_final)
                    clean_df2, folder_undone = check_df(folder_undone, gui_data['folder'], df_second_attempt)
                    clean_df2.to_excel(gui_data['folder'] + f'/organized_attempt2.xlsx', index=False)
    
                    clean_total = pd.concat([clean_df, clean_df2], ignore_index=True)
                    save_folder = gui_data['folder']
                else:
                    clean_total = clean_df
                    save_folder = os.path.dirname(single_file)
                    
                prog_bar.update_progress(95, desc=f"Second run: Saving Excel...")  

                clean_total.to_excel(save_folder + f'/FINAL.xlsx', index=False)
                
                end1 = time.time()
                total_time = end1 - start
                print(f"\n Total processing time: {total_time:.2f}")
                    
                prog_bar.update_progress(100, desc='Second run: Conversion successful, opening Excel...')
                app.processEvents()
                # path, extension = os.path.splitext(gui_data['folder'])
                os.startfile(save_folder + f'/FINAL.xlsx')
                sleep(2)
    
    def process_single_file(self, file_path):
        """Process a single PDF file where each page represents one CV."""
        pdf_reader = PdfReader(file_path)
        pdf_writer = PdfWriter()
        page_count = len(pdf_reader.pages)
        print(f"Processing {page_count} pages from the file: {file_path}")
    
        base_temp_dir = os.getcwd()  # Use the current working directory
        temp_folder = os.path.join(base_temp_dir, f"temp_{datetime.now().strftime('%Y%m%d%H%M%S')}")
        os.makedirs(temp_folder, exist_ok=True)
    
        # Save each page as a separate temporary file
        for i, page in enumerate(pdf_reader.pages):
            single_page_pdf = f"{temp_folder}/page_{i+1}.pdf"
            pdf_writer = PdfWriter()
            pdf_writer.add_page(page)
            with open(single_page_pdf, "wb") as f:
                pdf_writer.write(f)
    
        # Reuse batch processing logic
        global folder
        folder = temp_folder
        self.process_multiple_files(temp_folder, sf=True) 
        rmtree(temp_folder)
        
    def submit(self):
        # Check if '1 FILE' is selected and handle accordingly
        if self.one_file_check.isChecked():
            try:
                if not single_file.endswith('.pdf'):
                    print('Please select a valid PDF file.')
                    return
            except NameError:
                print('No file selected. Please browse for a file.')
                return
            self.close()  # Close the window
            self.process_single_file(single_file)  # Use self to call the method
        else:
            try:
                folder_path = folder  # Folder path set during folder browsing
                if not folder_path:
                    print('No folder selected. Please browse for a folder.')
                    return
            except NameError:
                print('No folder selected. Please browse for a folder.')
                return
            self.close()  # Close the window
            self.process_multiple_files(folder_path)  # Use self to call the method
   

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    
    terminate_subprocesses()
    sys.exit(app.exec())