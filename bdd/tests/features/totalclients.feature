Feature: Generate DataFrame with total number of clients
    As CEO,
    I want to calculate the total number of clients
    So that I can see the numbers on my report.

    Scenario: Count total number of clients
        Given client_details.csv

        When the source DataFrame is passed to the count total clients process

        Then the DataFrame should have a count of the total number of clients

    