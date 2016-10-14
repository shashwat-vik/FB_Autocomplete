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

probable_count = 0

class processGraph(Extra):
    def __init__(self):
        self.KEYS_FB = ['201401230292577|cfdd6ad8e4a9a6b8e19c2ac6ac9f1ab4',     # Shashwat
                        '1040517959329660|c7e458dd968fca33d05a18fddbcd86ab'     # Rohit
                        ]
        self.key_index=0

        self.graph = GraphAPI(self.KEYS_FB[self.key_index])

        # FOR STATE DATA TO BE USED BY AUTOCOMPLETE
        self.state_data_rows = []
        file_name = glob.glob('../../scrap-preprocessor/state_data/city_state.csv')
        state_file = open(file_name[0],'r')
        state_reader = csv.DictReader(state_file, dialect=csv.excel)
        self.state_data_rows.extend(state_reader)
        state_file.close()

    def get_state(self,city):
        state = ''
        found = False

        for row in self.state_data_rows:
            if row['Name of City'].strip().lower() == city.strip().lower():
                state = row['State']
                found = True
                break
        if not found:
            print 'NO STATE MATCH FOR CITY'
            sys.exit()
        else:
            return state

    def number_parser(self,x):
        flag_add = False
        numerals = ['0','1','2','3','4','5','6','7','8','9']
        allowed_start_symbols = numerals + ['+']

        ############
        #INITIAL CLEANUP
        x = x.strip()
        idx=0
        for _ in x:
            if _ in allowed_start_symbols:
                break
            idx += 1
        x = x[idx:]
        #############

        if x.find('+91') == 0 or x.find('91 ') == 0:
            flag_add = True

        word = ''
        phone_number = []
        if flag_add:
            word = list(x[3:])
        else:
            word = list(x)

        non_zero_encountered = False
        for letter in word:
            # REMOVES 0 FROM START OF NUMBERS
            if not non_zero_encountered:
                if letter in numerals[1:]:
                    non_zero_encountered = True

            if non_zero_encountered:
                if letter in numerals:
                    phone_number.append(letter)
        return ''.join(phone_number)

    def website_parser(self,x):
        if x == '' or x is None:
            return ''
        ############
        #INITIAL CLEANUP
        x = x.strip()
        x = x.replace('//www.','//')
        #############

        filler_flag = False
        fillers = ['/','#']
        for _ in fillers:
            if _ in x[-1]:
                filler_flag = True
        if filler_flag:
            x = x[:-1]
        return x

    def match_website(self,website,resp):
        print '\tMATCHING WEBSITE'
        print '\t || ORIGINAL : ',website
        print '\t || NEW : ',resp.encode('ascii',errors='ignore')

        if website == self.website_parser(resp):
            return True
        return False

    def match_phone_nos(self,phones,resp):
        print '\tMATCHING PHONES'

        # DECREASING SEPARATOR PRIORITY ORDER
        separators = [',', '/', ';', '&']

        resp_nos = []
        sep_found = False
        for separator in separators:
            if resp.find(separator) != -1:
                resp_nos.extend(resp.split(separator))
                sep_found = True
                break
        if not sep_found:
            resp_nos.append(resp)

        for i in range(len(resp_nos)):
            resp_nos[i] = resp_nos[i].encode('ascii',errors='ignore')


        print '\t || ORIGINAL : ',phones
        print '\t || NEW : ',resp_nos
        for x in resp_nos:
            if self.number_parser(x) in phones:
                print '\tPHONE MATCHED ***'
                return True
        print '\tNO PHONE MATCHED'
        return False

    def analyze_prediction(self,row,query,allow_website_match):
        print '\tQUERY : ',query
        probable = False
        city,pin=row['City'].lower(),row['Pincode']

        phones = []
        website = ''
        email = ''

        for i in range(1,6):
            if row['Phone'+str(i)]:
                phones.append(self.number_parser(row['Phone'+str(i)]))
        if row['Website']:
            website = self.website_parser(row['Website'])
        if row['Mail']:
            email = row['Mail'].strip()

        search_result=self.graph.get("search?q=%s&fields=location,phone,emails,website&type=place&limit=50"%(query))
        for place in search_result['data']:
            if 'location' in place:
                if 'zip' in place['location'] :
                    if unicode(pin) == unicode(place['location']['zip']) and unicode(pin):
                        print '\t PINCODE MATCHED ***'
                        node = self.graph.get(place['id']+"?fields=name,location,is_verified,description,phone,link,cover,website,emails")
                        print json.dumps(node,indent=4,cls=DecimalEncoder)
                        return True,False,node
                        #return self.graph.get(place['id']+"?fields=location,is_verified,description,phone,link,cover,website,emails")

                if 'city' in place['location'] :
                    if city == unicode(place['location']['city'].lower()) and not probable:
                        probable = place['id']
                        node = self.graph.get(place['id']+"?fields=name,location,is_verified,description,phone,link,cover,website,emails")
            #'''
            if 'phone' in place and phones:
                if self.match_phone_nos(phones,place['phone']):
                    node = self.graph.get(place['id']+"?fields=name,location,is_verified,description,phone,link,cover,website,emails")
                    print json.dumps(node,indent=4,cls=DecimalEncoder)
                    return True,False,node
                    #return self.graph.get(place['id']+"?fields=location,is_verified,description,phone,link,cover,website,emails")

            if 'emails' in place and email:
                for x in place['emails']:
                    if x == email:
                        print '\tEMAIL MATCHED ***'
                        node = self.graph.get(place['id']+"?fields=name,location,is_verified,description,phone,link,cover,website,emails")
                        print json.dumps(node,indent=4,cls=DecimalEncoder)
                        return True,False,node
            #'''
        if allow_website_match:
            match=False
            multiple_match=False
            correct_place_id=''

            for place in search_result['data']:
                if 'website' in place and website:
                    if self.match_website(website,place['website']):
                        if not match:
                            correct_place_id=place['id']
                            match=True
                        else:
                            multiple_match=True
                            break

            if match and not multiple_match:
                print '\tWEBSITE MATCHED @@@'
                node = self.graph.get(correct_place_id+"?fields=name,location,is_verified,description,phone,link,cover,website,emails")
                print json.dumps(node,indent=4,cls=DecimalEncoder)
                return True,False,node
            elif multiple_match:
                print '\tMULTIPLE MATCHED @@@'
            else:
                print '\tNO WEBSITE MATCHED'

        if probable:
            node = self.graph.get(probable+"?fields=location,description,is_verified,phone,link,cover,website,emails")
            return False,True,node
        return False,False,dict()


    def searchPlace(self,row,state):
        global probable_count

        matched=False
        probable=False
        node = ''

        query = row['Name']
        matched,probable,node=self.analyze_prediction(row,query,False)

        #'''
        if not matched and row['Locality']:
            print '\t# CHANGING QUERY(1)'
            query = row['Name'] + ', ' + row['Locality']
            matched,probable,node=self.analyze_prediction(row,query,True)
        #'''

        #'''
        if not matched:
            print '\t# CHANGING QUERY(2)'
            query = row['Name'] + ', ' + state
            matched,probable,node=self.analyze_prediction(row,query,True)
        #'''

        if matched:
            return node
        elif probable:
            probable_count+=1
            return node
        else:
            return dict()

    def processAll(self,rows):
        global probable_count
        details,link,cover,website,pincode,street,dp,verified,phone,email=0,0,0,0,0,0,0,0,0,0 #stats
        total = len(rows)

        state = self.get_state(rows[0]['City'])
        print 'STATE : ',state

        responses_received = 0
        no_response_received = 0
        print("Fetching info from FB Graph")
        for progress,row in enumerate(rows):
            print progress+2,' --> ',row['Name']
            try:
                node = self.searchPlace(row,state)
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
                #pro=int((float(progress)/total)*100)
                #sys.stdout.write("\r%d%%"%pro)
                #sys.stdout.flush()
            except:
               logging.exception("Error loading information from facebook for " + row['Name'])
            print ''
        #sys.stdout.write("\r100%")
        #sys.stdout.flush()
        print "\nNew Info Added from Facebook\nDetails:%d Facebook Link:%d Cover:%d \nWebsite:%d Pincode:%d Address:%d Images:%d Verified %d/%d Phone:%d Emails:%d"%(details,link,cover,website,pincode,street,dp,verified,link,phone,email)

        print '\nMATCHED : ',responses_received-probable_count
        print 'PROBABLE : ',probable_count
        print 'NO MATCH : ',no_response_received

def get_data():
    rows = []
    file_name = glob.glob('../../scrap-preprocessor/input/scrap_rajkot_output_1.csv')
    inputFile = open(file_name[0],'r')
    reader = csv.DictReader(inputFile,dialect=csv.excel)
    rows.extend(reader)
    inputFile.close()
    return rows

if __name__ == '__main__':
    print '\n\n\n\n'
    p = processGraph()
    p.processAll(get_data())
