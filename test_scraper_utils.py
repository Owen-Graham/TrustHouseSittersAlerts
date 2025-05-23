import unittest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

# Assuming scraper.py is in the same directory or accessible in PYTHONPATH
from scraper import (
    normalize_pet,
    split_location,
    escape_markdown,
    listing_id_from_url,
    format_date_for_url,
    apply_profile_filters,
    PET_TYPES # Import PET_TYPES as it's used by apply_profile_filters
)

class TestScraperUtils(unittest.TestCase):

    def test_normalize_pet(self):
        self.assertEqual(normalize_pet("Small Pet"), "small_pets")
        self.assertEqual(normalize_pet("small pet"), "small_pets")
        self.assertEqual(normalize_pet("  dog  "), "dog")
        self.assertEqual(normalize_pet("Horse"), "horse")
        self.assertEqual(normalize_pet("poultry"), "poultry")
        self.assertEqual(normalize_pet("  LiVeStoCK  "), "livestock")
        self.assertEqual(normalize_pet("  small_pets  "), "small_pets") # Already normalized
        self.assertEqual(normalize_pet("Exotic Bird"), "exotic_bird") # Test with space

    def test_split_location(self):
        self.assertEqual(split_location("Town, Country"), ("Town", "Country"))
        self.assertEqual(split_location("City Only"), ("City Only", ""))
        self.assertEqual(split_location("  Leading, Trailing  "), ("Leading", "Trailing"))
        self.assertEqual(split_location("Complex Name, Country with Spaces"), ("Complex Name", "Country with Spaces"))
        self.assertEqual(split_location("Another City"), ("Another City", ""))
        self.assertEqual(split_location("  Yet Another Place, Some Region  "), ("Yet Another Place", "Some Region"))
        self.assertEqual(split_location("Place,Region,With,Extra,Commas"), ("Place,Region,With,Extra", "Commas"))

    def test_escape_markdown(self):
        self.assertEqual(escape_markdown("*bold*"), "\\*bold\\*")
        self.assertEqual(escape_markdown("_italic_"), "\\_italic\\_")
        self.assertEqual(escape_markdown("[link](url)"), "\\[link\\]\\(url\\)")
        self.assertEqual(escape_markdown("`code`"), "\\`code\\`")
        self.assertEqual(escape_markdown(">quote"), "\\>quote")
        self.assertEqual(escape_markdown("#header"), "\\#header")
        self.assertEqual(escape_markdown("+plus"), "\\+plus")
        self.assertEqual(escape_markdown("-minus"), "\\-minus")
        self.assertEqual(escape_markdown("=equals"), "\\=equals")
        self.assertEqual(escape_markdown("|pipe"), "\\|pipe")
        self.assertEqual(escape_markdown("{curly}"), "\\{curly\\}")
        self.assertEqual(escape_markdown("}curly"), "\\}curly")
        self.assertEqual(escape_markdown(".dot!bang"), "\\.dot\\!bang")
        self.assertEqual(escape_markdown("text_with_underscore"), "text\\_with\\_underscore")
        self.assertEqual(escape_markdown("text*with*asterisk"), "text\\*with\\*asterisk")
        self.assertEqual(escape_markdown("normal text"), "normal text")
        self.assertEqual(escape_markdown(""), "")
        # Test with non-string input (should return as is based on current implementation)
        self.assertEqual(escape_markdown(123), 123)
        self.assertIsNone(escape_markdown(None))

    def test_listing_id_from_url(self):
        self.assertEqual(listing_id_from_url("https://site.com/house-and-pet-sitting-assignments/l/12345/"), "12345")
        self.assertEqual(listing_id_from_url("https://site.com/l/67890"), "67890")
        self.assertEqual(listing_id_from_url("https://site.com/l/123/"), "123")
        self.assertEqual(listing_id_from_url("https://site.com/l/456?param=value"), "456")
        self.assertEqual(listing_id_from_url("https://site.com/no_id_here/"), "https://site.com/no_id_here/")
        self.assertEqual(listing_id_from_url("https://site.com/l/"), "https://site.com/l/") # Invalid but testing regex
        self.assertEqual(listing_id_from_url("https://site.com/l/abc/"), "https://site.com/l/abc/") # Non-numeric ID

    def test_format_date_for_url(self):
        self.assertEqual(format_date_for_url("01 Nov 2025"), "2025-11-01")
        self.assertEqual(format_date_for_url("24 Dec 2025"), "2025-12-24")
        self.assertEqual(format_date_for_url("5 Jan 2023"), "2023-01-05") # Single digit day
        self.assertEqual(format_date_for_url(""), "") # Empty string
        self.assertEqual(format_date_for_url("Invalid Date"), "") # Invalid format
        self.assertEqual(format_date_for_url(None), "") # None input

    def test_apply_profile_filters(self):
        # Initialize sample data ensuring all PET_TYPES are columns
        sample_columns = ['listing_id', 'country', 'town', 'public_transport', 'car_included'] + PET_TYPES
        sample_data_list = [
            # UK, 1 dog, 0 cat
            {'listing_id': '1', 'country': 'UK', 'town': 'London', 'public_transport': True, 'car_included': False, 'dog': 1, 'cat': 0, 'horse': 0, 'bird': 0, 'fish': 0, 'rabbit': 0, 'reptile': 0, 'poultry': 0, 'livestock': 0, 'small_pets': 0},
            # France, 0 dog, 1 cat
            {'listing_id': '2', 'country': 'France', 'town': 'Paris', 'public_transport': False, 'car_included': True, 'dog': 0, 'cat': 1, 'horse': 0, 'bird': 0, 'fish': 0, 'rabbit': 0, 'reptile': 0, 'poultry': 0, 'livestock': 0, 'small_pets': 0},
            # Germany, 2 dogs, 1 cat
            {'listing_id': '3', 'country': 'Germany', 'town': 'Berlin', 'public_transport': True, 'car_included': True, 'dog': 2, 'cat': 1, 'horse': 0, 'bird': 0, 'fish': 0, 'rabbit': 0, 'reptile': 0, 'poultry': 0, 'livestock': 0, 'small_pets': 0},
            # UK, 1 dog, 1 small_pets
            {'listing_id': '4', 'country': 'UK', 'town': 'Manchester', 'public_transport': False, 'car_included': False, 'dog': 1, 'cat': 0, 'horse': 0, 'bird': 0, 'fish': 0, 'rabbit': 0, 'reptile': 0, 'poultry': 0, 'livestock': 0, 'small_pets': 1},
             # Spain, 0 dogs, 0 cats, 3 horses
            {'listing_id': '5', 'country': 'Spain', 'town': 'Madrid', 'public_transport': False, 'car_included': False, 'dog': 0, 'cat': 0, 'horse': 3, 'bird': 0, 'fish': 0, 'rabbit': 0, 'reptile': 0, 'poultry': 0, 'livestock': 0, 'small_pets': 0},
        ]
        
        # Fill missing PET_TYPES with 0 for rows in sample_data_list
        for row_dict in sample_data_list:
            for pet_type_col in PET_TYPES:
                if pet_type_col not in row_dict:
                    row_dict[pet_type_col] = 0
        
        sample_df = pd.DataFrame(sample_data_list, columns=sample_columns)

        # Test case 1: No filters
        profile_config_none = {}
        filtered_df_none = apply_profile_filters(sample_df.copy(), profile_config_none)
        assert_frame_equal(filtered_df_none.reset_index(drop=True), sample_df.reset_index(drop=True))

        # Test case 2: Exclude UK
        profile_config_exclude_uk = {"filters": {"excluded_countries": ["UK"]}}
        filtered_df_exclude_uk = apply_profile_filters(sample_df.copy(), profile_config_exclude_uk)
        self.assertEqual(len(filtered_df_exclude_uk[filtered_df_exclude_uk['country'] == 'UK']), 0)
        self.assertTrue('France' in filtered_df_exclude_uk['country'].values)
        self.assertEqual(len(filtered_df_exclude_uk), 3) # France, Germany, Spain

        # Test case 3: Exclude multiple countries
        profile_config_exclude_multi = {"filters": {"excluded_countries": ["UK", "Germany"]}}
        filtered_df_exclude_multi = apply_profile_filters(sample_df.copy(), profile_config_exclude_multi)
        self.assertEqual(len(filtered_df_exclude_multi[filtered_df_exclude_multi['country'] == 'UK']), 0)
        self.assertEqual(len(filtered_df_exclude_multi[filtered_df_exclude_multi['country'] == 'Germany']), 0)
        self.assertEqual(len(filtered_df_exclude_multi), 2) # France, Spain

        # Test case 4: Max 1 dog
        profile_config_max_dog_1 = {"filters": {"max_pets": {"dog": 1}}}
        filtered_df_max_dog_1 = apply_profile_filters(sample_df.copy(), profile_config_max_dog_1)
        self.assertTrue(all(filtered_df_max_dog_1['dog'] <= 1))
        self.assertEqual(len(filtered_df_max_dog_1), 4) # Excludes listing '3' (Germany, 2 dogs)

        # Test case 5: Max 0 cats
        profile_config_max_cat_0 = {"filters": {"max_pets": {"cat": 0}}}
        filtered_df_max_cat_0 = apply_profile_filters(sample_df.copy(), profile_config_max_cat_0)
        self.assertTrue(all(filtered_df_max_cat_0['cat'] == 0))
        self.assertEqual(len(filtered_df_max_cat_0), 3) # Excludes '2' (France) and '3' (Germany)

        # Test case 6: Max 1 dog AND max 0 cats
        profile_config_max_dog_cat = {"filters": {"max_pets": {"dog": 1, "cat": 0}}}
        filtered_df_max_dog_cat = apply_profile_filters(sample_df.copy(), profile_config_max_dog_cat)
        self.assertTrue(all(filtered_df_max_dog_cat['dog'] <= 1))
        self.assertTrue(all(filtered_df_max_dog_cat['cat'] == 0))
        # Expected:
        # 1 (UK, 1 dog, 0 cat) - PASS
        # 2 (France, 0 dog, 1 cat) - FAIL (cat > 0)
        # 3 (Germany, 2 dogs, 1 cat) - FAIL (dog > 1 and cat > 0)
        # 4 (UK, 1 dog, 0 small_pets (cat is 0)) - PASS
        # 5 (Spain, 0 dog, 0 cat, 3 horses) - PASS
        self.assertEqual(len(filtered_df_max_dog_cat), 3) 
        self.assertTrue('1' in filtered_df_max_dog_cat['listing_id'].values)
        self.assertTrue('4' in filtered_df_max_dog_cat['listing_id'].values)
        self.assertTrue('5' in filtered_df_max_dog_cat['listing_id'].values)


        # Test case 7: Max pets for a type not present in data (e.g. max 0 rabbit)
        profile_config_max_rabbit_0 = {"filters": {"max_pets": {"rabbit": 0}}}
        filtered_df_max_rabbit_0 = apply_profile_filters(sample_df.copy(), profile_config_max_rabbit_0)
        # Should not filter any rows as all rabbit counts are 0
        self.assertEqual(len(filtered_df_max_rabbit_0), len(sample_df))

        # Test case 8: Excluded country and max pets
        profile_config_exclude_and_max = {
            "filters": {
                "excluded_countries": ["Germany"],
                "max_pets": {"dog": 0}
            }
        }
        filtered_df_exclude_and_max = apply_profile_filters(sample_df.copy(), profile_config_exclude_and_max)
        # Excludes Germany (listing '3')
        # Then from remaining, keeps only those with dog == 0
        # 1 (UK, 1 dog) - FAIL (dog > 0)
        # 2 (France, 0 dog) - PASS
        # 4 (UK, 1 dog) - FAIL (dog > 0)
        # 5 (Spain, 0 dog) - PASS
        self.assertEqual(len(filtered_df_exclude_and_max), 2)
        self.assertTrue('2' in filtered_df_exclude_and_max['listing_id'].values) # France
        self.assertTrue('5' in filtered_df_exclude_and_max['listing_id'].values) # Spain
        self.assertTrue(all(filtered_df_exclude_and_max['country'] != 'Germany'))
        self.assertTrue(all(filtered_df_exclude_and_max['dog'] == 0))

        # Test case 9: Filter not in profile_config (empty filters dict)
        profile_config_empty_filters = {"filters": {}}
        filtered_df_empty_filters = apply_profile_filters(sample_df.copy(), profile_config_empty_filters)
        assert_frame_equal(filtered_df_empty_filters.reset_index(drop=True), sample_df.reset_index(drop=True))
        
        # Test case 10: max_pets with a pet type not in PET_TYPES (should be ignored)
        profile_config_unknown_pet = {"filters": {"max_pets": {"unicorn": 0}}}
        filtered_df_unknown_pet = apply_profile_filters(sample_df.copy(), profile_config_unknown_pet)
        assert_frame_equal(filtered_df_unknown_pet.reset_index(drop=True), sample_df.reset_index(drop=True))

if __name__ == '__main__':
    unittest.main()
