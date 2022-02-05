Feature: Generate DataFrame
    As a user,
    I want to do some transformations on my DataFrame,
    So that I can get new data from it.

    Scenario: Add country code
        Given the source data

        When the source DataFrame is passed to the add country code process

        Then the DataFrame should have an extra column with the country code

    Scenario: Add dynamic country code
        Given the source data

        When the source DataFrame is passed to the add country code process with "country" as the column name and "NL" as the country

        Then the DataFrame should have an extra column with the correct name and country code

    Scenario: Add values
        Given the source data

        When the source DataFrame is passed to the add values process with value columns to add "value1,value2" and the new column named "values_added"

        Then the DataFrame should have an extra column with the values added

    Scenario: Conditions
        Given the source data

        When the source DataFrame is passed to the values_condition method with a <column_name> equal to <equal_to> the result should be <should_be> otherwise it must be <otherwise> and the resulting column name is <resulting_column_name>

        Then the DataFrame should have an extra column with the condition column added

        Examples:
        | column_name | equal_to | should_be | otherwise | resulting_column_name |
        |  value1   |  1  |  Yes   | No | validation1 |
        |  value2   |  4  |  Yes   | No | validation2 |

