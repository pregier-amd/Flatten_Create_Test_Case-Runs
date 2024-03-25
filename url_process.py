import requests
import os
class UrlProcess():
    # Read URL
    def get_file(self,url=""):
        #url = 'https://www.facebook.com/favicon.ico'
        r = requests.get(url, allow_redirects=True)
        return r.content
    def curl_get(self,url='',outfile="test.log"):
        return os.system("curl " + url + " --output " + outfile)

if __file__ == '__main__':
    u = UrlProcess()
    print ( u.get_file('http://mpdwww.amd.com//lwa_workarea/CCX_Diag/Nirvana/BRH/A0/CDL_IT_Logs/Dense/Turin_Dense_EA6D/01_Sep_2023-01_27_11/') )

