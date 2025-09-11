#!/usr/bin/env python3
"""
Enhanced batch document extractor for INDEX I and INDEX II historical documents
"""

import requests
import json
import time
from datetime import datetime
import os
import base64
import sqlite3
import re

def base64_encode_image(file_path):
    """Encode local image to base64 for vLLM payload"""
    with open(file_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def extract_json_from_response(content):
    """Extract JSON from model response, handling markdown wrapping and extra text"""
    try:
        # First, try direct parsing
        return json.loads(content.strip())
    except json.JSONDecodeError:
        pass
    
    # Try to find JSON in markdown code blocks
    json_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
    match = re.search(json_pattern, content, re.DOTALL | re.IGNORECASE)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    
    # Try to find JSON by looking for { and }
    content_lines = content.strip().split('\n')
    json_start = -1
    json_end = -1
    
    for i, line in enumerate(content_lines):
        if line.strip().startswith('{'):
            json_start = i
            break
    
    for i in range(len(content_lines)-1, -1, -1):
        if content_lines[i].strip().endswith('}'):
            json_end = i
            break
    
    if json_start != -1 and json_end != -1:
        json_text = '\n'.join(content_lines[json_start:json_end+1])
        try:
            return json.loads(json_text)
        except json.JSONDecodeError:
            pass
    
    # If all else fails, return None
    return None

def extract_document(image_path, index_type="INDEX_2", debug=False):
    """Extract document data with configurable index type"""
    
    # Your vLLM API endpoint (local)
    api_url = "http://localhost:8001/v1/chat/completions"
    
    # Update the prompt based on index type
    if index_type == "INDEX_1":
        prompt_text = """
GEOGRAPHICAL KNOWLEDGE FOR 1960s WEST BENGAL:

DISTRICTS AND SUBDIVISIONS:
- Hooghly: Chinsurah, Serampore, Chandernagore, Arambagh, Khanakul
- Burdwan: Ausgram, Kalna, Katwa, Dainhat, Memari, Jamalpur
- 24 Parganas: Barasat, Basirhat, Diamond Harbour, Alipore, Baruipur, Canning
- Nadia: Krishnanagar, Ranaghat, Kalyani, Tehatta, Chapra
- Murshidabad: Berhampore, Kandi, Lalbagh, Hariharpara, Nawda
- Birbhum: Suri, Bolpur, Rampurhat, Sainthia, Mayurbhanj
- Bankura: Bankura, Bishnupur, Khatra, Taldangra, Onda
- Purulia: Purulia, Raghunathpur, Jhalda, Para, Baghmundi
- Midnapore: Midnapore, Tamluk, Contai, Jhargram, Ghatal
- Jalpaiguri: Jalpaiguri, Siliguri, Kalimpong, Alipurduar
- Darjeeling: Darjeeling, Kurseong, Mirik
- Cooch Behar: Cooch Behar, Tufanganj, Mathabhanga, Mekhliganj
- West Dinajpur: Balurghat, Raiganj, Islampur, Kushmandi
- Malda: English Bazar, Chanchal, Ratua, Habibpur

COMMON POLICE STATIONS (PS):
Ausgram, Chinsurah, Serampore, Chandernagore, Berhampore, Krishnanagar, Ranaghat, 
Suri, Bolpur, Bankura, Bishnupur, Midnapore, Tamluk, Jalpaiguri, Siliguri, 
Balurghat, Raiganj, Arambagh, Kalna, Katwa, Memari, Barasat, Basirhat

COMMON RELIGIONS:
Hindu, Muslim, Christian, Buddhist, Sikh, Jain, Brahmo

COMMON OCCUPATIONS (1960s):
Cultivator, Trader, Weaver, Blacksmith, Carpenter, Teacher, Clerk, Zamindar, 
Shopkeeper, Fisherman, Boatman, Goldsmith, Tailor, Barber, Washerman, 
Potter, Milkman, Palanquin Bearer, Cooly, Service, Business, Retired

COMMON MISSPELLINGS:
- "Ausgram" often misread as "Ausgrama"
- "Katwa" as "Katra"

Use this knowledge to correct common OCR errors in place names, police stations, 
religions, and occupations. Apply phonetic matching for Bengali names.

Extract data from this INDEX I document. Return ONLY JSON - no explanations, no reasoning.

COLUMN STRUCTURE FOR INDEX I:
1. "Name of person" → Primary person involved in the transaction
2. "Family details" → Relationship information of the person (look for S/o, D/o, W/o)
3. "Interest of person" → Legal interest or role in the transaction
4. "Where registered" → Name of the place where the registration occurred
5. "Serial number" → Sequential entry number
6. "Book 1 Volume" → Volume number (Roman or Arabic numerals)
7. "Book 2 Page" → Page number reference

ENHANCED FAMILY DETAILS PARSING:
From the "Family details" field, extract these components separately:
- Police Station (PS): Look for "PS [name]" pattern (e.g., "PS Ausgram")
- Religion: Hindu, Muslim, Christian, Buddhist, Sikh, Jain, Brahmo
- Occupation: Cultivator, Trader, Weaver, etc. (often at the end)
- Original text: Keep the full original family details text

Example: "S/o Ses Ismail of Jamtaa PS Ausgram, Burdwan Muslim Cultivator"
Should extract:
- family_details: "S/o Ses Ismail of Jamtaa PS Ausgram, Burdwan Muslim Cultivator"
- police_station: "Ausgram"
- religion: "Muslim" 
- occupation: "Cultivator"

HANDWRITING INTERPRETATION FOR NAMES AND RELATIONSHIPS:
- Bengali names often have variations: Ram/Rom, Nandi/Uandi, Krishna/Kvishna
- Common relationships: S/o (son of), D/o (daughter of), W/o (wife of)
- 'a'→'o', 'n'→'u', 'r'→'v', 'l'→'t' variations are common
- Numbers: '1'→'l', '5'→'S', '0'→'O', '6'→'G', '9'→'g'
- Use geographical knowledge to correct place names and PS names

EXTRACTION GUIDELINES:
- Extract ALL entries row by row systematically
- For unclear names: [UNCLEAR: best_guess_name]
- For illegible text: [ILLEGIBLE]
- For ditto marks (do, ", —): Write the actual entry value (e.g., name of the person or location)
- Pay special attention to relationship indicators (S/o, D/o, W/o)
- Use geographical knowledge to correct OCR errors in place names
- Extract PS, religion, and occupation from family details when present

Return data in this JSON format:
{
  "document_type": "INDEX_1",
  "year": "extracted year",
  "office_location": "extracted location",
  "entries": [
    {
      "serial_number": "entry number",
      "name_of_person": "full person name",
      "family_details": "full relationship information",
      "police_station": "PS name if found, otherwise null",
      "religion": "religion if found, otherwise null",
      "occupation": "occupation if found, otherwise null",
      "interest_of_person": "legal interest or role",
      "where_registered": "name of the place",
      "book_1_volume": "volume number",
      "book_2_page": "page number"
    }
  ],
  "confidence": "high/medium/low",
  "extraction_notes": "observations about handwriting, name interpretations, address corrections"
}

Focus on accurate name extraction, relationship identification, and enhanced parsing of PS, religion, and occupation for genealogical purposes.
        """
    else:
        prompt_text = """
GEOGRAPHICAL KNOWLEDGE FOR 1960s WEST BENGAL:

DISTRICTS AND SUBDIVISIONS:
- Hooghly: Chinsurah, Serampore, Chandernagore, Arambagh, Khanakul
- Burdwan: Ausgram, Kalna, Katwa, Dainhat, Memari, Jamalpur
- 24 Parganas: Barasat, Basirhat, Diamond Harbour, Alipore, Baruipur, Canning
- Nadia: Krishnanagar, Ranaghat, Kalyani, Tehatta, Chapra
- Murshidabad: Berhampore, Kandi, Lalbagh, Hariharpara, Nawda
- Birbhum: Suri, Bolpur, Rampurhat, Sainthia, Mayurbhanj
- Bankura: Bankura, Bishnupur, Khatra, Taldangra, Onda
- Purulia: Purulia, Raghunathpur, Jhalda, Para, Baghmundi
- Midnapore: Midnapore, Tamluk, Contai, Jhargram, Ghatal
- Jalpaiguri: Jalpaiguri, Siliguri, Kalimpong, Alipurduar
- Darjeeling: Darjeeling, Kurseong, Mirik
- Cooch Behar: Cooch Behar, Tufanganj, Mathabhanga, Mekhliganj
- West Dinajpur: Balurghat, Raiganj, Islampur, Kushmandi
- Malda: English Bazar, Chanchal, Ratua, Habibpur

COMMON POLICE STATIONS (PS):
Ausgram, Chinsurah, Serampore, Chandernagore, Berhampore, Krishnanagar, Ranaghat, 
Suri, Bolpur, Bankura, Bishnupur, Midnapore, Tamluk, Jalpaiguri, Siliguri, 
Balurghat, Raiganj, Arambagh, Kalna, Katwa, Memari, Barasat, Basirhat

COMMON RELIGIONS:
Hindu, Muslim, Christian, Buddhist, Sikh, Jain, Brahmo

COMMON OCCUPATIONS (1960s):
Cultivator, Trader, Weaver, Blacksmith, Carpenter, Teacher, Clerk, Zamindar, 
Shopkeeper, Fisherman, Boatman, Goldsmith, Tailor, Barber, Washerman, 
Potter, Milkman, Palanquin Bearer, Cooly, Service, Business, Retired

COMMON MISSPELLINGS:
- "Ausgram" often misread as "Ausgrama"
- "Katwa" as "Katra"

Use this knowledge to correct common OCR errors in place names, police stations, 
religions, and occupations. Apply phonetic matching for Bengali names.

Extract data from this INDEX II document. Return ONLY JSON - no explanations, no reasoning.

COLUMN STRUCTURE FOR INDEX II:
1. "Serial number" → Sequential entry number
2. "Property name" → General information about the property
3. "Pargana/Town/Thana" → Location details
4. "Location" → District and sub-district information
5. "Nature of transaction" → Type of transaction (sale, mortgage, lease, gift, etc.)
6. "Where registered" → Registration office location
7. "Book 1 Volume" → Volume number (Roman or Arabic numerals)
8. "Book 1 Page" → Page number reference

PROPERTY DESCRIPTION INTERPRETATION:
- It might have the "Khatian" or "Khatian" number or "Kh" number. Unit is in "Satak" or "Bigha" mostly.
- Look for plot numbers, survey numbers, boundaries (North, South, East, West)
- Property types: house, land, garden, tank, road, etc.
- Measurements in local units: bigha, katha, chhatak, ganda
- Boundary descriptions: "bounded by", "adjacent to", "touching"
- Use geographical knowledge to correct place names in property descriptions

TRANSACTION TYPES:
- Sale deed, mortgage, lease, gift, partition, exchange
- Legal terminology in Bengali/English mixed script

EXTRACTION GUIDELINES:
- Extract ALL entries row by row systematically
- For unclear property descriptions: [UNCLEAR: best_guess_description]
- For illegible locations: [ILLEGIBLE]
- For ditto marks (do, ", —): Write the actual entry value (e.g., name of the place or location)
- Pay attention to property boundaries and measurements
- Use geographical knowledge to correct OCR errors in location names

Return data in this JSON format:
{
  "document_type": "INDEX_2",
  "year": "extracted year",
  "office_location": "extracted location",
  "entries": [
    {
      "serial_number": "entry number",
      "property_name": "general information about the property",
      "Pargana/Town/Thana": "location details",
      "location": "district and sub-district information",
      "nature_of_transaction": "transaction type",
      "where_registered": "registration office location",
      "book_1_volume": "volume number",
      "book_1_page": "page number"
    }
  ],
  "confidence": "high/medium/low",
  "extraction_notes": "observations about property descriptions, location interpretations, transaction types identified"
}

Focus on accurate property description and location identification for historical land records.
Use geographical knowledge to ensure place names are correctly identified.
        """
    
    # Base64 encode the image
    try:
        image_data = f"data:image/jpeg;base64,{base64_encode_image(image_path)}"
    except Exception as e:
        print(f"❌ Error encoding image {image_path}: {e}")
        return {
            "source_path": image_path,
            "processed_at": datetime.now().isoformat(),
            "index_type": index_type,
            "error": f"Image encoding error: {str(e)}"
        }
    
    # The payload - FIXED MODEL NAME
    payload = {
        "model": "/data/models/gemma-3-12b-it",  # ← FIXED: Use the correct model path
        "temperature": 0.2,
        "top_p": 0.5,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt_text
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": image_data
                        }
                    }
                ]
            }
        ]
    }
    
    try:
        print(f"🔄 Processing {index_type} document: {os.path.basename(image_path)}")
        
        response = requests.post(
            api_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=300
        )
        
        response.raise_for_status()
        result = response.json()
        
        # Extract content
        if "choices" in result and len(result["choices"]) > 0:
            content = result["choices"][0]["message"]["content"]
            
            if debug:
                print("✅ Response received!")
                print(f"📝 Raw content (first 200 chars): {content[:200]}...")
            
            # Try to extract JSON from the response
            json_data = extract_json_from_response(content)
            
            if json_data is not None:
                if debug:
                    print("✅ Successfully parsed as JSON!")
                
                # Add metadata
                json_data["source_path"] = image_path
                json_data["processed_at"] = datetime.now().isoformat()
                json_data["index_type"] = index_type
                
                # Insert to SQLite
                try:
                    conn = sqlite3.connect('/data/db/extraction.db')
                    cursor = conn.cursor()
                    
                    # Insert document metadata
                    cursor.execute('''
                        INSERT INTO documents (document_type, year, office_location, confidence, extraction_notes, source_path)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (json_data.get('document_type'), json_data.get('year'), json_data.get('office_location'), json_data.get('confidence'), json_data.get('extraction_notes'), image_path))
                    document_id = cursor.lastrowid
                    
                    # Insert entries
                    for entry in json_data.get('entries', []):
                        if index_type == "INDEX_1":
                            cursor.execute('''
                                INSERT INTO index1_entries (document_id, serial_number, name_of_person, family_details, police_station, religion, occupation, interest_of_person, where_registered, book_1_volume, book_2_page)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (document_id, entry.get('serial_number'), entry.get('name_of_person'), entry.get('family_details'), entry.get('police_station'), entry.get('religion'), entry.get('occupation'), entry.get('interest_of_person'), entry.get('where_registered'), entry.get('book_1_volume'), entry.get('book_2_page')))
                        else:
                            cursor.execute('''
                                INSERT INTO index2_entries (document_id, serial_number, property_name, pargana_town_thana, location, nature_of_transaction, where_registered, book_1_volume, book_1_page)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (document_id, entry.get('serial_number'), entry.get('property_name'), entry.get('Pargana/Town/Thana'), entry.get('location'), entry.get('nature_of_transaction'), entry.get('where_registered'), entry.get('book_1_volume'), entry.get('book_1_page')))
                    
                    conn.commit()
                    conn.close()
                    
                    if debug:
                        print(f"✅ Data saved to database with {len(json_data.get('entries', []))} entries")
                    
                except Exception as db_error:
                    print(f"⚠️ Database error: {db_error}")
                    json_data["database_error"] = str(db_error)
                
                return json_data
            else:
                print(f"❌ Failed to parse JSON from response")
                if debug:
                    print(f"📝 Raw content: {content}")
                return {
                    "source_path": image_path,
                    "processed_at": datetime.now().isoformat(),
                    "index_type": index_type,
                    "raw_content": content,
                    "parsing_error": True,
                    "error": "Could not extract valid JSON from response"
                }
        else:
            print("❌ No response content found")
            return {
                "source_path": image_path,
                "processed_at": datetime.now().isoformat(),
                "index_type": index_type,
                "error": "No response content found"
            }
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error processing {image_path}: {e}")
        return {
            "source_path": image_path,
            "processed_at": datetime.now().isoformat(),
            "index_type": index_type,
            "error": f"Request error: {str(e)}"
        }
    except Exception as e:
        print(f"❌ Unexpected error processing {image_path}: {e}")
        return {
            "source_path": image_path,
            "processed_at": datetime.now().isoformat(),
            "index_type": index_type,
            "error": f"Unexpected error: {str(e)}"
        }

def process_batch_documents(index1_folder, index2_folder, delay_seconds=2, debug=False):
    """Process all documents in both index folders"""
    
    results = {
        "batch_info": {
            "processed_at": datetime.now().isoformat(),
            "total_index1_docs": 0,
            "total_index2_docs": 0,
            "total_docs": 0
        },
        "index1_results": [],
        "index2_results": [],
        "summary": {
            "index1_successful": 0,
            "index1_failed": 0,
            "index2_successful": 0,
            "index2_failed": 0
        }
    }
    
    # List files
    try:
        index1_files = [os.path.join(index1_folder, f) for f in os.listdir(index1_folder) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        index2_files = [os.path.join(index2_folder, f) for f in os.listdir(index2_folder) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        # Sort files for consistent processing order
        index1_files.sort()
        index2_files.sort()
        
    except Exception as e:
        print(f"❌ Error listing files: {e}")
        return results
    
    results["batch_info"]["total_index1_docs"] = len(index1_files)
    results["batch_info"]["total_index2_docs"] = len(index2_files)
    results["batch_info"]["total_docs"] = len(index1_files) + len(index2_files)
    
    print("🚀 Starting batch document extraction...")
    print(f"📊 Total documents to process: {results['batch_info']['total_docs']}")
    print(f"📁 INDEX I files: {len(index1_files)}")
    print(f"📁 INDEX II files: {len(index2_files)}")
    
    # Process INDEX I
    if index1_files:
        print("\n📋 Processing INDEX I documents...")
        for i, path in enumerate(index1_files, 1):
            print(f"\n[{i}/{len(index1_files)}] INDEX I Document: {os.path.basename(path)}")
            result = extract_document(path, "INDEX_1", debug=debug)
            results["index1_results"].append(result)
            if "error" in result or result.get("parsing_error"):
                results["summary"]["index1_failed"] += 1
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")
            else:
                results["summary"]["index1_successful"] += 1
                entries_count = len(result.get('entries', []))
                print(f"✅ Success: {entries_count} entries extracted")
            if i < len(index1_files):
                print(f"⏳ Waiting {delay_seconds} seconds...")
                time.sleep(delay_seconds)
    else:
        print("\n📋 No INDEX I documents found")
    
    # Process INDEX II
    if index2_files:
        print("\n📋 Processing INDEX II documents...")
        for i, path in enumerate(index2_files, 1):
            print(f"\n[{i}/{len(index2_files)}] INDEX II Document: {os.path.basename(path)}")
            result = extract_document(path, "INDEX_2", debug=debug)
            results["index2_results"].append(result)
            if "error" in result or result.get("parsing_error"):
                results["summary"]["index2_failed"] += 1
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")
            else:
                results["summary"]["index2_successful"] += 1
                entries_count = len(result.get('entries', []))
                print(f"✅ Success: {entries_count} entries extracted")
            if i < len(index2_files):
                print(f"⏳ Waiting {delay_seconds} seconds...")
                time.sleep(delay_seconds)
    else:
        print("\n📋 No INDEX II documents found")
    
    # Print summary
    print("\n" + "="*60)
    print("📊 BATCH PROCESSING COMPLETE!")
    print("="*60)
    print(f"✅ INDEX I: {results['summary']['index1_successful']} successful, {results['summary']['index1_failed']} failed")
    print(f"✅ INDEX II: {results['summary']['index2_successful']} successful, {results['summary']['index2_failed']} failed")
    total_successful = results['summary']['index1_successful'] + results['summary']['index2_successful']
    total_failed = results['summary']['index1_failed'] + results['summary']['index2_failed']
    print(f"🎯 TOTAL: {total_successful} successful, {total_failed} failed")
    
    if total_failed > 0:
        print(f"\n⚠️ {total_failed} documents failed. Check the output above for error details.")
    
    # Save batch results to file
    batch_results_file = f"/data/batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(batch_results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"📄 Batch results saved to: {batch_results_file}")
    except Exception as e:
        print(f"⚠️ Could not save batch results: {e}")
    
    return results

def test_single_document(image_path, index_type="INDEX_1"):
    """Test extraction on a single document with debug output"""
    print(f"🧪 Testing single document extraction...")
    print(f"📄 File: {image_path}")
    print(f"📋 Type: {index_type}")
    print("-" * 50)
    
    result = extract_document(image_path, index_type, debug=True)
    
    print("\n" + "="*50)
    print("🧪 SINGLE DOCUMENT TEST COMPLETE!")
    print("="*50)
    
    if "error" in result or result.get("parsing_error"):
        print(f"❌ Test failed: {result.get('error', 'Unknown error')}")
    else:
        entries_count = len(result.get('entries', []))
        print(f"✅ Test successful: {entries_count} entries extracted")
        
        # Save result to file
        test_result_file = f"/data/test_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(test_result_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print(f"📄 Test result saved to: {test_result_file}")
        except Exception as e:
            print(f"⚠️ Could not save test result: {e}")
    
    return result

# Main execution (for standalone run, but use Airflow for pipeline)
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            # Test mode - single document
            image_path = sys.argv[2] if len(sys.argv) > 2 else "/data/index1/index1_3.jpg"
            index_type = sys.argv[3] if len(sys.argv) > 3 else "INDEX_1"
            test_single_document(image_path, index_type)
        else:
            print("Usage: python3 script.py [test] [image_path] [index_type]")
    else:
        # Batch mode - all documents
        process_batch_documents("/data/index1/", "/data/index2/", debug=False)