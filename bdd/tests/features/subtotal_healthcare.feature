Feature: Generate DataFrame with subtotal of clients that use JCB creditcards in the Healthcare sector
    As CEO,
    I want to calculate the subtotal of clients in the healthcare sector that use JCB creditcards
    So that I can see the numbers on my report.

    Scenario:Filter clients in the healtchare sector
        Given client_details.csv

        When the source DataFrame is passed to the filter clients in the healthcare sector

        Then the DataFrame should contain the clients in the healthcare sector
        
    Scenario:Filter clients that us JCB creditcards
        Given filtered clients in the healthcare sector

        When the source DataFrame is passed to the filtered clients in the healthcare sector

        Then the DataFrame hould contain the clients in the healthcare sector that use JCB creditcards

    Scenario: Count clients in healthcare sectors that use JCB creditcards
        Given filtered clients in the healthcare sector that use JCB creditcards

        When the source DataFrame is passed to the filtered clients in the healthcare sector that use JCB creditcards

        Then the DataFrame should have a count of the total number of clients in the healthcare sector that use JCB creditcards.

    