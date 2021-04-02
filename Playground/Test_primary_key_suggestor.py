from DataCitationFramework.QueryUtils import suggest_primary_key
import pandas as pd

# Test with real data from the new dataset
test_df = pd.read_csv("dem_and_rep_party_members_mentions.csv", delimiter=',')
print(suggest_primary_key(test_df))

# Test 1: duplicate rows
test_df = pd.read_csv("test_primary_key_suggestor_ds1.csv", delimiter=',')
print("test 1: ", suggest_primary_key(test_df))

# Test 2: Multiple possible composite keys
# Output: two possible keys of tuple size 2. User needs to give feedback on which primary key to take
test_df = pd.read_csv("test_primary_key_suggestor_ds2.csv", delimiter=',')
print("test 2: ", suggest_primary_key(test_df))

# Test 3: Column names changed compared to test 1 so that a sort of column names would yield a different order
# for datasets in test 1 and test 3
# Output: Same output as test 2, just the column names are different. Also order of key attributes within a tuple
# did not change semantically and can be mapped to the column names in test 2
test_df = pd.read_csv("test_primary_key_suggestor_ds3.csv", delimiter=',')
print("test 3: ", suggest_primary_key(test_df))

# Test 4: Test 3 + Column names are permuted
# Output: Different order of tuple attributes than for test 3. User needs to chose the primary key and define the
# order
test_df = pd.read_csv("test_primary_key_suggestor_ds4.csv", delimiter=',')
print("test 4: ", suggest_primary_key(test_df, True))

# Test 5: Test 3 + Column names are permuted + suggested keys have different number of
# distinct stacked key attributes values
# Output: Same key suggestions as for test 4
test_df = pd.read_csv("test_primary_key_suggestor_ds5.csv", delimiter=',')
print("test 5: ", suggest_primary_key(test_df))