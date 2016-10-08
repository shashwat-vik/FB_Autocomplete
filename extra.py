def UTF8(data):
    try:
        return data.encode('utf-8','ignore')
    except:
        return data

class Extra():
    def _repairDetails(self,row,node):
        if 'description' in node and not row['Details']:
            row['Details'] = node['description']
            #print "Added description "+node['description'][:40]+" to "+row["Name"]+" from facebook"
            return 1
        return 0

    def _repairWebsite(self,row,node):
        if not row['Website']:
            if 'website' in node:
                row['Website'] = node['website']
                #print "Added website "+node['website']+" to "+row["Name"]+" from facebook"
                return 1
        return 0

    def _repairPin(self,row,node):
        if 'location' in node:
            if not row['Pincode'] and 'zip' in node['location'] :
                row['Pincode'] = node['location']['zip']
                #print "Added pin "+node['location']['zip']+" to "+row["Name"]+" from facebook"
                return 1
        return 0

    def _repairStreet(self,row,node):
        if 'location' in node:
            if not row['Street Address'] and 'street' in node['location']:
                row['Street Address'] = node['location']['street']
                #print "Added address "+node['location']['street']+" to "+row["Name"]+" from facebook"
                return 1
        return 0

    def _addPage(self,row,node):
        if 'link' in node:
            row['fb_page']= node['link']
            #print "Added page "+node['link']+" to "+row["Name"]+" from facebook"
            return 1
        return 0

    def _isVerified(self,row,node):
        if 'is_verified' in node:
            if node['is_verified']:
                row['fb_verified']= 'True'
                return 1
            row['fb_verified']= 'False'
        return 0

    def _addCover(self,row,node):
            if 'cover' in node:
                row['Images URL'] = node['cover']['source']+","+row['Images URL']
                #print "Added cover "+node['cover']['source']+" to "+row["Name"]+" from facebook"
                return 1
            return 0
    def _addEmails(self,row,node):
        if 'emails' in node:
##            p=2
##            for i in node['emails']:
##                if i != row['Mail']:
##                    row['Mail'+str(p)] = i
##                    #print "Added mail "+i+" to "+'Mail'+str(p)+" from facebook"
##                    p+=1
##            return p-2
            email2 = node['emails'][0]
            if email2 != row['Mail']:
                row['Mail2'] = email2
                #print "Added mail "+node['emails'][0]
                return 1
        return 0
    def _addPhone(self,row,node):
        if 'phone' in node:
            ph = map(UTF8,node['phone'].split(','))
            for i in range(1,6):
                if not row['Phone'+str(i)]:
                    break
            for j,p in zip(range(i+1,6),ph):
                row['Phone'+str(j)] = p.strip()
                #print "Added phone "+p.strip()+" in "+'Phone'+str(j)+" from facebook"+str(node['location'])
            return 1
        return 0



    def _addPicture(self,row,node):
        if not 'id' in node:
            return 0
        profile_pic=self.graph.get(node['id']+"/picture?height=500&width=500&redirect")
        if 'data' in profile_pic:
            if 'url' in profile_pic['data'] and 'is_silhouette' in profile_pic['data']:
                if not profile_pic['data']['is_silhouette']:
                    row['Images URL'] += profile_pic['data']['url']+","
                    return 1
        return 0
