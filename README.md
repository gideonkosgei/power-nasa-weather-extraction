<!-- PROJECT LOGO -->
<br />
<div align="center">  

<h3 align="center">Weather & Climatology Data Extractor</h3>

  
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>   
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project
This tool extracts weather & climatology data by making requests to the POWER NASA Web Service. It runs on python and data is stored on a MySQL database.
For faster Turnaround time, the tools has multiprocessing capabilities

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With

* Python
* MySQL




<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites
* Python
  ```sh
  sudo apt update
  sudo apt install python3.8
  python --version  
  ```
  
* MySQL
  ```sh
  sudo apt update
  sudo apt install mysql-server
  sudo systemctl start mysql.service  
  ```

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/gideonkosgei/power-nasa-weather-extraction.git
   
2. Install Python Dependencies
   ```sh
   pip install -r requirements.txt
   ```


<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Usage
### Groupings
The groups are dependent on the user community and temporal level.
Groupings enables parameter recommendations to be tailored to each of the communities:
- [ ] Sustainable Buildings (SB)
- [ ] Agroclimatology (AG)
- [ ] Renewable Energy (RE)

While within each community, parameters are filtered by the temporal availability (Hourly, Daily, Monthly, or Climatological) 

### Types

* Temporal: The APIs that return user community specific Analysis Ready Data products.
* Application: The APIs that return user specified reports and validation products that use the Temporal APIs.
* System: The APIs that supply consistent configuration information across the APIs.


### Response Times 
The response times vary between the different services and load a given time.

    • As a general rule the higher the temporal level (Hourly vs a Climatology) and the greater number of parameters requested will slightly slow down the 's response.
    • The Application 's are the slowest to respond; they typically include multiple temporal data request that run simultaneously. Additionally, have more intensive back end processing, but they requests will complete in under a minute.

### Temporal APIs
The Temporal APIs that return user community specific Analysis Ready Data products.

* Climatology : Provides parameters as climatologies for a pre-defined period with monthly average, maximum, and/or minimum values available.
* Monthly : Provides parameters by year; the annual and each month's average, maximum, and/or minimum values.
* Daily : Provides parameters by day with average, maximum, and/or minimum values.
* Hourly : Provides parameters by hour with average values.


### Spatial


* Point:
The Point endpoint returns a time series based on a single latitude and longitude coordinate across the time span requested.
* Regional:
The Regional endpoint returns a time series based on a bounding box of lower left (latitude, longitude) and upper right (latitude, longitude) coordinates across the time span provided.
* Global:
The Global endpoint returns long term climatological averages for the entire globe.

### Daily
The Daily microservice returns time series analysis ready data responses for solar and meteorological data to be used directly by applications

  ```sh
   /api/temporal/daily/point?parameters=T2M&community=SB&longitude=0&latitude=0&start=20170101&end=20170201&format=JSON

   ```
```sh
/api/temporal/daily/regional?latitude-min=50&latitude-max=55&longitude-min=50&longitude-max=55&parameters=T2M&community=SB&start=20170101&end=20170201&format=CSV

   ```



### Monthly and Annual
The Monthly and Annual microservice returns time series analysis ready data responses for solar and meteorological data to be used directly by applications.

Request Structure

```sh
/api/temporal/monthly/point?parameters=T2M,T2M_MAX&community=SB&longitude=0&latitude=0&format=JSON&start=2016&end=2017
 ```

### Multi-Point Download with Python
This is an overview of the process to request data from multiple single point locations.
* The Loop (Points) tab below allows for the download directly from the for any format offered.
* The Multiprocessing (Points) and Multiprocessing (Region) support multiprocessing and read the data from the JSON response pulling the data into a pandas dataframe prior to being exported as a . This can be edited to create any format with Python and/or the Pandas module

### Notes On When Requesting Data:
    • Please ensure you do not submit too many synchronous requests; if it is determined that you are negatively impacting server performance you potentially will be blocked.
    • Please ensure that you properly request data from the Data Services at no higher than its current resolution. The  data products are currently available at 0.5 x 0.625 degree resolution for meteorology and 1 x 1 for solar parameters. If you are requesting data at a too high a resolution (i.e, less than 0.5 degree or about 50 km), you are potentially repetitively requesting the same information.
    • Within two to three (2-3) months after real-time, the meteorological data products are replaced with improved climate quality data products. This needs to be taken into account when downloading the  Data Archive.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- CONTACT -->
## Contact

Gideon Kosgei - mr.gkosgei@gmail.com

Project Link: [https://github.com/gideonkosgei/power-nasa-weather-extraction](https://github.com/gideonkosgei/power-nasa-weather-extraction)

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [The POWER Project](https://power.larc.nasa.gov/)


<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/github_username/repo_name.svg?style=for-the-badge
[contributors-url]: https://github.com/github_username/repo_name/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/github_username/repo_name.svg?style=for-the-badge
[forks-url]: https://github.com/github_username/repo_name/network/members
[stars-shield]: https://img.shields.io/github/stars/github_username/repo_name.svg?style=for-the-badge
[stars-url]: https://github.com/github_username/repo_name/stargazers
[issues-shield]: https://img.shields.io/github/issues/github_username/repo_name.svg?style=for-the-badge
[issues-url]: https://github.com/github_username/repo_name/issues
[license-shield]: https://img.shields.io/github/license/github_username/repo_name.svg?style=for-the-badge
[license-url]: https://github.com/github_username/repo_name/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/linkedin_username
[product-screenshot]: images/screenshot.png
[Next.js]: https://img.shields.io/badge/next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white
[Next-url]: https://nextjs.org/
[Next-url]: https://nextjs.org/
[React.js]: https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB
[React-url]: https://reactjs.org/
[Vue.js]: https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D
[Vue-url]: https://vuejs.org/
[Angular.io]: https://img.shields.io/badge/Angular-DD0031?style=for-the-badge&logo=angular&logoColor=white
[Angular-url]: https://angular.io/
[Svelte.dev]: https://img.shields.io/badge/Svelte-4A4A55?style=for-the-badge&logo=svelte&logoColor=FF3E00
[Svelte-url]: https://svelte.dev/
[Laravel.com]: https://img.shields.io/badge/Laravel-FF2D20?style=for-the-badge&logo=laravel&logoColor=white
[Laravel-url]: https://laravel.com
[Bootstrap.com]: https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white
[Bootstrap-url]: https://getbootstrap.com
[JQuery.com]: https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white
[JQuery-url]: https://jquery.com 