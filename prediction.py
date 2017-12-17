# -*- coding: utf-8 -*-
"""
Created on Sat Nov 18 09:04:56 2017

@author: Sreenivas
"""

import torch
from torch.autograd import Variable
import torchvision.transforms as transforms
import torch.nn as nn
import torch.nn.functional as F
import imagedataset as Image
import sys
import pandas as pd
labelfile=['drink','food','inside','menu','outside']
#Prediction of images
def prediction(model,imageloader):
    pred=[]
    #feeding input to network and extracting predictions
    for batch_idx,(data) in enumerate(imageloader):
        output=model(Variable(data))
        _,predicted=torch.max(output.data,1)
        print(predicted[0][0])
        pred.append(labelfile[predicted[0][0]])
    return pred
class Net(nn.Module):
    def __init__(self):
        super(Net,self).__init__()
        self.conv1=nn.Conv2d(3,5,5)
        self.pool=nn.MaxPool2d(2)
        self.conv2=nn.Conv2d(5,5,5)
        self.fc1=nn.Linear(5*53*53,20)
        self.fc2=nn.Linear(20,25)
        self.fc3=nn.Linear(25,5)
    def forward(self,x):
        x=self.pool(F.relu(self.conv1(x)))
        x=self.pool(F.relu(self.conv2(x)))
        x=x.view(-1,5*53*53)
        x=F.relu(self.fc1(x))
        x=F.dropout(x,training=self.training)
        x=F.relu(self.fc2(x))
        x=F.dropout(x,training=self.training)
        x=self.fc3(x)
        return F.log_softmax(x)
#Main function
if __name__=="__main__":
    #Load modelfile,imagefolder,labelfile
    modelfile=sys.argv[1]
    imagefolder=sys.argv[2]
    photoidfile=sys.argv[3]
    model=torch.load(modelfile)
    #Transformation of inpyt image into tensor of range 0...1
    normalize=transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
    transform=transforms.Compose([transforms.RandomSizedCrop(224),
            transforms.ToTensor(),
            normalize])
    #Forming imagedataset
    imagedataset=Image.Imagedataset(imagefolder,photoidfile,transform)
    imageloader=torch.utils.data.DataLoader(imagedataset)
    #list of prediction
    labellist=prediction(model,imageloader)
    #saving results to csv file
    labeldf=pd.DataFrame(labellist)
    labeldf.to_csv('prediction.csv',index=False)
        