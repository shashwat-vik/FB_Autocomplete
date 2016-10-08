import csv
import glob
import json, decimal
from facepy import GraphAPI
import logging, sys
from extra import Extra

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
            #return super(DecimalEncoder,self).default(o)
        return super(DecimalEncoder,self).default(o)

class processGraph(Extra):
    def __init__(self):
        self.KEYS_FB = ['201401230292577|cfdd6ad8e4a9a6b8e19c2ac6ac9f1ab4',     # Shashwat
                        '1040517959329660|c7e458dd968fca33d05a18fddbcd86ab'     # Rohit
                        ]
        self.key_index=0

        self.graph = GraphAPI(self.KEYS_FB[self.key_index])

    def searchPlace(self,row):
        name,city,pin=row['Name'],row['City'].lower(),row['Pincode']
        city=unicode(city.lower())

        query = name
        search_result=self.graph.get("search?q=%s&fields=location&type=place&limit=50"%(query))
        probable = None
        for place in search_result['data']:
            if not 'location' in place:
                continue
            if 'zip' in place['location'] :
                if unicode(pin) == unicode(place['location']['zip'])  and unicode(pin):
                    return self.graph.get(place['id']+"?fields=location,is_verified,description,phone,link,cover,website,emails")
            if 'city' in place['location'] :
                if city == unicode(place['location']['city'].lower()) and not probable:
                    probable = place['id']
        if probable:
            return self.graph.get(probable+"?fields=location,description,is_verified,phone,link,cover,website,emails")
        return dict()

    def processAll(self,rows):
        details,link,cover,website,pincode,street,dp,verified,phone,email=0,0,0,0,0,0,0,0,0,0 #stats
        total = len(rows)

        responses_received = 0
        no_response_received = 0
        print("Fetching info from FB Graph")
        for progress,row in enumerate(rows):
            try:
                node = self.searchPlace(row)
                if node:
                    responses_received += 1
                else:
                    no_response_received += 1

                details += self._repairDetails(row,node)
                website += self._repairWebsite(row,node)
                pincode += self._repairPin(row,node)
                street += self._repairStreet(row,node)
                link += self._addPage(row,node)
                cover += self._addCover(row,node)
                dp += self._addPicture(row,node)
                phone += self._addPhone(row,node)
                email += self._addEmails(row,node)
                verified += self._isVerified(row,node)
                pro=int((float(progress)/total)*100)
                sys.stdout.write("\r%d%%"%pro)
                sys.stdout.flush()
            except:
               logging.exception("Error loading information from facebook for " + row['Name'])
        sys.stdout.write("\r100%")
        sys.stdout.flush()
        print "\nNew Info Added from Facebook\nDetails:%d Facebook Link:%d Cover:%d \nWebsite:%d Pincode:%d Address:%d Images:%d Verified %d/%d Phone:%d Emails:%d"%(details,link,cover,website,pincode,street,dp,verified,link,phone,email)

        print '\nRECEIVED : ',responses_received
        print 'NOT-RECE : ',no_response_received

def get_data():
    rows = []
    file_name = glob.glob('../../scrap-preprocessor/input/sample.csv')
    inputFile = open(file_name[0],'r')
    reader = csv.DictReader(inputFile,dialect=csv.excel)
    rows.extend(reader)
    inputFile.close()
    return rows

if __name__ == '__main__':
    print '\n\n\n\n'
    p = processGraph()
    p.processAll(get_data())








    def check(self):
        name='IIT Delhi'
        query = 'search?q=%s&type=place&fields=location&limit=10'%(name)

        print 'QUERY : ',query
        search_result = self.graph.get(query)
        #print search_result
        print json.dumps(search_result,indent=4,cls=DecimalEncoder)

        idx = 1
        for place in search_result['data']:
            print '\n CHECKING %s : '%(idx),
            if 'zip' in place['location'] :
                print '\n'
                query = place['id']+"?fields=location,is_verified,description,phone,link,cover,website,emails"

                print 'QUERY : ',query
                search_result_x = self.graph.get(query)
                print json.dumps(search_result_x,indent=4,cls=DecimalEncoder)
            else:
                print 'SKIPPED'

            idx += 1

    #return row[]
    #address = row['Name'] + ', ' + row['Locality']
    #address = row['Name'] + ', ' + row['Pincode']
    #address = row['Name'] + ', ' + row['City']
    #address = row['Name'] + row['Street Address']
    #address = row["Street Address"] + ' ' + row["Locality"] + ', ' + row["City"]
