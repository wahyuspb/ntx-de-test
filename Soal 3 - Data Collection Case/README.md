# Data Collection Challenge for DE Technical Test

## Context
In this challenge, you'll be required to perform data scraping on a specified website, targeting specific levels and data pages. This exercise will test your ability to efficiently extract and handle web data using asynchronous methods and various tools/libraries.

## Instructions
1. **Set Up**:
    - Ensure that you have the required libraries installed. 
        ```bash
        pip install httpx BeautifulSoup4 polars tqdm
        ```

2. **Script Placement**:
    - Please save your Python script in the base folder of the project.

3. **Execution**: 
    - You need to perform data scraping for a total of 5 levels. The maximum pages for each level have been predefined in the `max_pages` list.
    - Your script should asynchronously request data from the URL `https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={i}` where `{level}` is the current level and `{i}` is the page number you are accessing.
    - For each level, you should extract the following information from the scraped data:
        - `title`: Title of the article.
        - `link`: Direct link to the article.
    - Ensure you handle exceptions and timeouts to prevent the script from crashing. It's evident in the provided script on how exceptions are handled with retries.

4. **Output**:
    - For each level, store the collected data as a CSV file named `forti_lists_{level}.csv` in a directory named `datasets/`.
    - Any skipped pages during scraping due to exceptions should be noted and stored in a JSON file named `skipped.json` in the `datasets/` directory. The JSON should contain the level and the tuple of skipped pages.

5. **Evaluation**:
    - Your script will be evaluated based on its efficiency, error handling, and the correctness of the scraped data.
    - Consider making use of asynchronous programming as shown in the provided script to enhance the speed and efficiency of the scraping process.
    
6. **Expected Output**:
    - After running the script, the `datasets/` directory should have 5 CSV files named `forti_lists_{level}.csv` for each level.
    - The `datasets/` directory should also contain a `skipped.json` file detailing any skipped pages during scraping.
    
Good Luck!