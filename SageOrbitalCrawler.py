'''
Project SAGE -- MINDS @ ISI, USC
Programming Assignment for Python Developer Student Worker

Name: Akshay Gulati
email: gulatiak@usc.edu
'''


from bs4 import BeautifulSoup

import time
import requests


class Util:

    @staticmethod
    def sanitizeDate(date):

        month_map = {

            'January': '01',
            'February': '02',
            'March': '03',
            'April': '04',
            'May': '05',
            'June': '06',
            'July': '07',
            'August': '08',
            'September': '09',
            'October': '10',
            'November': '11',
            'December': '12',
        }

        date = date.split('(')[0]
        date = date.split('[')[0]
        date = date.strip()
        date = date.split(' ')
        day = date[0]
        month = month_map[date[1]]
        return day, month
    

    @staticmethod
    def sanitizeResult(result):
        
        return result.split('[')[0].strip()
    

    @staticmethod
    def writeCSV(launchCount):

        monthDayCount = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        with open('OrbitalLaunchCount_2019.csv', 'w') as csv:

            csv.write('date, value')
            csv.write('\n')

            for month, dayCount in enumerate(monthDayCount):
                for day in range(1, dayCount + 1):
                    if len(launchCount) > 0:
                        if day == int(launchCount[0][0]) and month + 1 == int(launchCount[0][1]):
                            csv.write('2019-{0}-{1}T00:00:00+00:00, {2}'.format(launchCount[0][1], launchCount[0][0].zfill(2), launchCount[0][2]))
                            csv.write('\n')
                            launchCount = launchCount[1:]
                        
                        else:
                            csv.write('2019-{0}-{1}T00:00:00+00:00, {2}'.format(str(month + 1).zfill(2), str(day).zfill(2), 0))
                            csv.write('\n')
                    
                    else:
                            csv.write('2019-{0}-{1}T00:00:00+00:00, {2}'.format(str(month + 1).zfill(2), str(day).zfill(2), 0))
                            csv.write('\n')

        
class SageOrbitalCrawler:
    
    def __init__(self, maxRetries = 3, waitTime = 5.0, timeout = 3.0):

        self.timeout = timeout              # Max time (in seconds) to wait for web server to respond to our request
        self.waitTime = waitTime            # Time (in seconds) for crawler to wait before resending a request in case of error
        self.maxRetries = maxRetries        # Max attempts to resend requests in case of previous were error


    # Simple, recursive crawler retrying to crawl in case of timeout/error/status code != 200
    # Raises RuntimeError Exception in case the desired page in unreachable
    # Returns page HTML if page reached
    def __getSeedHTML(self, attemptIndex):
        
        _olSeedURL = 'https://en.wikipedia.org/wiki/2019_in_spaceflight'

        try:
            olPage = requests.get(_olSeedURL, timeout = self.timeout)

            if olPage.status_code != 200:
                if attemptIndex == self.maxRetries:
                    raise RuntimeError({
                        'message': 'Fatal Error: Could not access {0}'.format(_olSeedURL),
                        'error': 'Status Code: {0}'.format(olPage.status_code),
                        'attemptIndex': attemptIndex
                    })
                
                else:
                    time.sleep(self.waitTime)
                    self.__getSeedHTML(attemptIndex + 1)                

        except requests.exceptions.RequestException as err:

            if attemptIndex == self.maxRetries:
                    raise RuntimeError({
                        'message': 'Fatal Error: Could not access {0}'.format(_olSeedURL),
                        'error': err,
                        'attemptIndex': attemptIndex
                    })

            else:
                time.sleep(self.waitTime)
                self.__getSeedHTML(attemptIndex + 1)
        
        else:
            olHTML = olPage.text
            return olHTML
            
    # Extracts the distinct launch counts as per assignment specification from a given Wikipedia page HTML
    # Returns a Nx3 list with N->No. of total launches, 3-> [day, month, count]
    def __extractLaunchCount(self, html, allowed_results = ['Successful', 'Operational', 'En Route']):
        
        launchCount = []

        soup = BeautifulSoup(html, 'html5lib')
        tables = soup.find_all('table', attrs={'class': 'wikitable collapsible'})       # First wikitable collapsible is our table of interest

        olTable = tables[0]
        olTableRows = olTable.find_all('tr')

        prevLaunchCount = 0
        skipTillNextDate = False
        currDay, currMonth = None, None
        prevDay, prevMonth = Util.sanitizeDate(olTableRows[4].find_all('td')[0].find('span').get_text())    # Get first launch details

        for row in olTableRows[5:]:
            cols = row.find_all('td')
            if len(cols) == 5:                                                                              # First row is date and rocket details
                currDay, currMonth = Util.sanitizeDate(cols[0].find('span').get_text())
                skipTillNextDate = False

                if currDay != prevDay or currMonth != prevMonth:                                            # Found a new date, store previous date in result
                    launchCount.append([prevDay, prevMonth, prevLaunchCount])
                    prevLaunchCount = 0
                    prevDay = currDay
                    prevMonth = currMonth

            
            if len(cols) == 6 and skipTillNextDate == False:                                               # Rows with payload details
                result = Util.sanitizeResult(cols[5].get_text())
                if result in allowed_results:
                    prevLaunchCount = prevLaunchCount + 1
                    skipTillNextDate = True                                                               # Skip all other rows if one row has result in allowed_results
        
        if currDay != None and prevLaunchCount != 0:                                                    
            launchCount.append([currDay, currMonth, prevLaunchCount])

        return launchCount


    def run(self):

        html = self.__getSeedHTML(0)
        launchCount = self.__extractLaunchCount(html)
        return launchCount


if __name__ == "__main__":
    
    SOC = SageOrbitalCrawler()
    launchCount = SOC.run()
    Util.writeCSV(launchCount)
