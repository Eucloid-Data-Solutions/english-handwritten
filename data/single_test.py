#!/usr/bin/env python3
import requests
import json
import base64
from datetime import datetime

def base64_encode_image(file_path):
    with open(file_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def test_extract_debug(image_path, index_type="INDEX_1", output_file="test_output.json"):
    api_url = "http://localhost:8001/v1/chat/completions"
    
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

Extract data from this INDEX I document. Return ONLY JSON - no explanations, no reasoning.

COLUMN STRUCTURE FOR INDEX I:
1. "Name of person" → Primary person involved in the transaction
2. "Family details" → Relationship information of the person (look for S/o, D/o, W/o)
3. "Interest of person" → Legal interest or role in the transaction
4. "Where registered" → Name of the place where the registration occurred
5. "Serial number" → Sequential entry number
6. "Book 1 Volume" → Volume number (Roman or Arabic numerals)
7. "Book 2 Page" → Page number reference

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
      "interest_of_person": "legal interest or role",
      "where_registered": "name of the place",
      "book_1_volume": "volume number",
      "book_2_page": "page number"
    }
  ],
  "confidence": "high/medium/low"
}
        """
    else:
        # Similar simplified prompt for INDEX_2
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
    
    image_data = f"data:image/jpeg;base64,{base64_encode_image(image_path)}"
    
    payload = {
        "model": "/data/models/gemma-3-12b-it",
        "temperature": 0.2,
        "top_p": 0.5,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt_text}, {"type": "image_url", "image_url": {"url": image_data}}]}
        ]
    }
    
    try:
        print("🔄 Sending request to API...")
        response = requests.post(api_url, json=payload, timeout=300)
        response.raise_for_status()
        result = response.json()
        
        print("✅ Got response from API")
        print("📋 Full API response structure:")
        print(json.dumps(result, indent=2)[:1000] + "..." if len(str(result)) > 1000 else json.dumps(result, indent=2))
        
        content = result['choices'][0]['message']['content']
        print("\n📝 Raw content from model:")
        print("="*50)
        print(content)
        print("="*50)
        print(f"Content length: {len(content)} characters")
        print(f"First 100 chars: {repr(content[:100])}")
        
        # Try to find JSON in the response
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
            print(f"\n🔍 Extracted JSON (lines {json_start}-{json_end}):")
            print(json_text)
            
            try:
                json_data = json.loads(json_text)
                json_data['source_path'] = image_path
                json_data['processed_at'] = datetime.now().isoformat()
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                
                print(f"✅ JSON saved to {output_file}")
                return True
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing error: {e}")
                print(f"Error at position: {e.pos}")
                return False
        else:
            print("❌ No JSON block found in response")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error: {e}")
        return False
    except KeyError as e:
        print(f"❌ Response format error: {e}")
        print("Response keys:", list(result.keys()) if 'result' in locals() else "No result")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

if __name__ == "__main__":
    # Example usage
    success = test_extract_debug("/data/index1/index1_3.jpg", "INDEX_1", "test_index1.json")
    if success:
        print("\n🎉 Extraction completed successfully!")
    else:
        print("\n💥 Extraction failed - check the output above for details")