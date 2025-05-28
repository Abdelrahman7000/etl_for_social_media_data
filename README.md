<h2>Overview</h2>

This project implements an ETL pipeline for processing and analyzing social media data. The pipeline performs the following steps:
<ul>
    <li>Extract: Extracts and Ingests raw JSON data into MinIO, serving as a staging area.</li>
    <li>Transform: Uses PySpark to process and model the data into a star schema structure.</li>
    <li>Load (Data Warehouse): Loads the transformed data into PostgreSQL, which serves as the data warehouse.</li>
</ul>
<h5>Final Data Warehouse Schema</h5>
The schema follows a star schema design and includes the following tables:
<ul>
    <li> user_dim: Contains user-related information.</li>
    <li>post_dim: Stores details of social media posts.</li>
    <li>comment_dim: Includes metadata about comments on posts.</li>
    <li>calendar_dim: Date-related dimension for time-based analysis.</li>
    <li>engagement_fact: Central fact table capturing metrics like likes, shares, and comments, linked to the dimension tables.</li>
</ul>



<img src='https://github.com/user-attachments/assets/75cc558c-c826-4445-8d1b-08791f6ed0c6'>


<h2>How to Run the ETL Pipeline project</h2>
<h3>Requirements</h3>
<ul>
    <li>Docker compose</li>
    <li>Astro</li>
</ul>
To start all the docker containers run: <bold>astro dev start</bold>









