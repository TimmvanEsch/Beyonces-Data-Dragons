# Pytest BDD

## Some important things before starting:

- #### Gherkin 

    - https://cucumber.io/docs/gherkin/reference/
    - Created by the software tool called Cucumber (BDD Tool) to make it easier to write test scenarios.
    - It follows the following structure:

    ```feature
    Feature: Add numbers
        As a user,
        I want to have a process that adds a number to another number,
        So that I can make my boss happy.

        Scenario: Add one
            Given the number 2
    
            When I pass the number 2 to the add one process
    
            Then the I should receive the number 3    
  
        Scenario: Add two
            Given the number 2
    
            When I pass the number 2 to the add two process
    
            Then the I should receive the number 4
    ```

- #### Feature file

    - Using PytestBDD one should create a **features directory** where a file with extension **feature** should be kept and it's where one would write the scenarios.
    - Only one feature per feature file.

- #### Step file

    - Using PytestBDD one should also create a **steps directory**, this is used to put the test files and it's called steps because of the **steps** in each scenario.

> Python also has the **behave** library - https://behave.readthedocs.io/en/stable/ - that shares a lot of features with PytestBDD
