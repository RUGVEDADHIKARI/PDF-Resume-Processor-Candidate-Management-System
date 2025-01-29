from pdfminer.high_level import extract_text
import fitz
from PIL import Image
import os
import re
from datetime import datetime
import sqlite3
from contextlib import contextmanager
import threading

class ResumeProcessor:
    def __init__(self, db_path='candidate_data1.db'):
        self.db_path = db_path
        self._local = threading.local()
        self.create_database()

    @contextmanager
    def get_db_connection(self):
        """Thread-safe database connection context manager"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(self.db_path)
        try:
            yield self._local.connection
        except Exception as e:
            if hasattr(self._local, 'connection'):
                self._local.connection.rollback()
            raise e

    def get_cursor(self):
        """Get a cursor for the current thread's database connection"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(self.db_path)
        return self._local.connection.cursor()

    def create_database(self):
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('PRAGMA foreign_keys = ON')
            
            # Drop tables in correct order
            tables = ['PDFData', 'Education', 'Skills', 'Experience', 'Candidate']
            for table in tables:
                cursor.execute(f'DROP TABLE IF EXISTS {table}')

            # Create tables
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Candidate (
                candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT,
                middle_name TEXT,
                last_name TEXT,
                permanent_address TEXT,
                current_address TEXT,
                date_of_birth DATE,
                age INTEGER,
                gender TEXT,
                passport_number TEXT UNIQUE,
                mobile TEXT,
                pan_number TEXT UNIQUE,
                visa_status TEXT,
                email TEXT UNIQUE,
                emergency_contact_name TEXT,
                emergency_contact_number TEXT,
                relocation_availability TEXT
            )''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Experience (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER,
                company TEXT,
                position TEXT,
                start_date DATE,
                end_date DATE,
                responsibilities TEXT,
                FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id) ON DELETE CASCADE
            )''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Skills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER,
                skill_name TEXT,
                proficiency TEXT,
                FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id) ON DELETE CASCADE
            )''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Education (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER,
                institution TEXT,
                degree TEXT,
                graduation_date DATE,
                gpa FLOAT,
                FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id) ON DELETE CASCADE
            )''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS PDFData (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id INTEGER,
                pdf_path TEXT,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (candidate_id) REFERENCES Candidate(candidate_id) ON DELETE CASCADE
            )''')

            conn.commit()

    def extract_text_content(self, pdf_path):
        try:
            text = extract_text(pdf_path)
            return self._clean_text(text)
        except Exception as e:
            print(f"Error extracting text: {e}")
            return None

    def extract_tables(self, pdf_path):
        """
        Optional table extraction - only if Java is properly configured
        """
        try:
            import tabula
            tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
            return tables
        except ImportError:
            print("Tabula-py not properly configured - skipping table extraction")
            return []
        except Exception as e:
            print(f"Error extracting tables: {e}")
            return []

    def extract_images(self, pdf_path, output_dir='extracted_images'):
        try:
            doc = fitz.open(pdf_path)
            os.makedirs(output_dir, exist_ok=True)
            images = []
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                image_list = page.get_images()
                
                for img_idx, img in enumerate(image_list):
                    xref = img[0]
                    base_image = doc.extract_image(xref)
                    image_bytes = base_image["image"]
                    
                    image_path = os.path.join(output_dir, f'page{page_num+1}_img{img_idx+1}.png')
                    with open(image_path, 'wb') as img_file:
                        img_file.write(image_bytes)
                    images.append(image_path)
            
            return images
        except Exception as e:
            print(f"Error extracting images: {e}")
            return []

    def extract_candidate_info(self, text):
        patterns = {
            'first_name': r'Name.*?\(Block Letters.*?\):.*?_(.*?)_.*?_.*?_.*?\(',
            'middle_name': r'Name.*?\(Block Letters.*?\):.*?_.*?_(.*?)_.*?_.*?\(',
            'last_name': r'Name.*?\(Block Letters.*?\):.*?_.*?_.*?_(.*?)_.*?\(',
            'permanent_street': r'2\.\s*Permanent Address:.*?Street Address:\s*_+(.*?)_+',
            'permanent_city': r'Permanent Address:.*?City:\s*_+(.*?)_+',
            'permanent_state': r'Permanent Address:.*?State:\s*_+(.*?)_+',
            'permanent_zip': r'Permanent Address:.*?Zip Code:\s*_+(.*?)_+',
            'permanent_country': r'Permanent Address:.*?Country:\s*_+(.*?)_+',
            'current_street': r'Current Address:.*?Street Address:\s*_+(.*?)_+',
            'current_city': r'Current Address:.*?City:\s*_+(.*?)_+',
            'current_state': r'Current Address:.*?State:\s*_+(.*?)_+',
            'current_zip': r'Current Address:.*?Zip Code:\s*_+(.*?)_+',
            'current_country': r'Current Address:.*?Country:\s*_+(.*?)_+',
            'dob': r'Date of Birth:\s*(\d{2}\s*/\s*\d{2}\s*/\s*\d{4})',
            'age': r'Age:\s*(\d+)',
            'gender': r'Gender:\s*([FM])',
            'passport': r'Passport:\s*(\w+)',
            'mobile': r'Mobile:\s*_*(\d+)_*',
            'pan': r'PAN No\.:\s*(\w+)',
            'visa': r'Visa:\s*([^_\n]+)',
            'email': r'Email ID:\s*([^\n_]+)',
            'emergency_contact': r'Name of Emergency Contact:\s*([^\n_]+)',
            'emergency_number': r'Emergency Contact\'s Number:\s*(\d+)',
            'relocation': r'Available for Relocation:\s*(\w+)'
        }
        
        info = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if match:
                info[key] = match.group(1).strip()
        
        return info

    def store_candidate_data(self, candidate_info, pdf_path):
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                permanent_address = ", ".join(filter(None, [
                    candidate_info.get('permanent_street', ''),
                    candidate_info.get('permanent_city', ''),
                    candidate_info.get('permanent_state', ''),
                    candidate_info.get('permanent_zip', ''),
                    candidate_info.get('permanent_country', '')
                ]))

                current_address = ", ".join(filter(None, [
                    candidate_info.get('current_street', ''),
                    candidate_info.get('current_city', ''),
                    candidate_info.get('current_state', ''),
                    candidate_info.get('current_zip', ''),
                    candidate_info.get('current_country', '')
                ]))

                cursor.execute('''
                    INSERT INTO Candidate (
                        first_name, middle_name, last_name,
                        permanent_address, current_address,
                        date_of_birth, age, gender,
                        passport_number, mobile, pan_number,
                        visa_status, email,
                        emergency_contact_name, emergency_contact_number,
                        relocation_availability
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    candidate_info.get('first_name'),
                    candidate_info.get('middle_name'),
                    candidate_info.get('last_name'),
                    permanent_address,
                    current_address,
                    candidate_info.get('dob'),
                    candidate_info.get('age'),
                    candidate_info.get('gender'),
                    candidate_info.get('passport'),
                    candidate_info.get('mobile'),
                    candidate_info.get('pan'),
                    candidate_info.get('visa'),
                    candidate_info.get('email'),
                    candidate_info.get('emergency_contact'),
                    candidate_info.get('emergency_number'),
                    candidate_info.get('relocation')
                ))
                
                candidate_id = cursor.lastrowid

                cursor.execute('''
                    INSERT INTO PDFData (candidate_id, pdf_path, processed_date)
                    VALUES (?, ?, ?)
                ''', (
                    candidate_id,
                    pdf_path,
                    datetime.now()
                ))

                conn.commit()
                return candidate_id
        except Exception as e:
            print(f"Error storing data: {e}")
            return None

    def get_all_candidates(self):
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT c.*, p.processed_date 
                    FROM Candidate c 
                    LEFT JOIN PDFData p ON c.candidate_id = p.candidate_id
                ''')
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching candidates: {e}")
            return []

    def get_candidate_by_id(self, candidate_id):
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT c.*, p.processed_date 
                    FROM Candidate c 
                    LEFT JOIN PDFData p ON c.candidate_id = p.candidate_id
                    WHERE c.candidate_id = ?
                ''', (candidate_id,))
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                return dict(zip(columns, row)) if row else None
        except Exception as e:
            print(f"Error fetching candidate: {e}")
            return None

    def delete_candidate(self, candidate_id):
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM Candidate WHERE candidate_id = ?', (candidate_id,))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error deleting candidate: {e}")
            return False

    def _clean_text(self, text):
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def process_pdf(self, pdf_path):
        print(f"Processing PDF: {pdf_path}")
        
        text = self.extract_text_content(pdf_path)
        if text:
            print("Text extraction successful")
            candidate_info = self.extract_candidate_info(text)
            
            # Optional table extraction
            tables = self.extract_tables(pdf_path)
            if tables:
                print(f"Found {len(tables)} tables")
            
            images = self.extract_images(pdf_path)
            if images:
                print(f"Extracted {len(images)} images")
            
            candidate_id = self.store_candidate_data(candidate_info, pdf_path)
            if candidate_id:
                print(f"Successfully stored candidate data with ID: {candidate_id}")
                return candidate_info
            else:
                print("Failed to store candidate data")
                return None
        else:
            print("Text extraction failed")
            return None

    def __del__(self):
        if hasattr(self._local, 'connection'):
            self._local.connection.close()