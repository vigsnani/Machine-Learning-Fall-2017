# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 18:36:57 2017

@author: Sreenivas
"""
import torch
from torch.autograd import Variable
import torchvision.transforms as transforms
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import argparse
import imagelabel as Image
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix,f1_score
#Training Settings
parser = argparse.ArgumentParser(description='Image classification')
parser.add_argument('--data', metavar='DIR',
                    help='path to image directory')
parser.add_argument('--label',metavar='FILE',help='path to label file')
parser.add_argument('--batchsize', type=int, default=64, metavar='N',
                    help='input batch size for training (default: 64)')
parser.add_argument('--testbatchsize', type=int, default=100, metavar='N',
                    help='input batch size for testing (default: 1000)')
parser.add_argument('--validbatchsize', type=int, default=64, metavar='N',
                    help='input batch size for validation (default: 64)')
parser.add_argument('--epochs', type=int, default=100, metavar='N',
                    help='number of epochs to train (default: 10)')
parser.add_argument('--lr', type=float, default=0.01, metavar='LR',
                    help='learning rate (default: 0.01)')
parser.add_argument('--momentum', type=float, default=0.5, metavar='M',
                    help='SGD momentum (default: 0.5)')
#I ahve made no cuda default True->change when gpu is set up
parser.add_argument('--no-cuda', action='store_true', default=False,
                    help='disables CUDA training')
parser.add_argument('--seed', type=int, default=1, metavar='S',
                    help='random seed (default: 1)')
parser.add_argument('--log-interval', type=int, default=10, metavar='N',
                    help='how many batches to wait before logging training status')

#Buiding convolutional neural network
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
#Transformation functions
def train_transformation(train_dataset,transform):
    train_transform=[]
    for value in train_dataset:
        train_transform.append((transform(value[0]),value[1]))
    return train_transform
def test_transformation(test_dataset,transform):
    test_transform=[]
    for value in test_dataset:
        test_transform.append((transform(value[0]),value[1]))
    return test_transform
def validation_transformation(test_dataset,transform):
    valid_transform=[]
    for value in test_dataset:
        valid_transform.append((transform(value[0]),value[1]))
    return valid_transform
#Train function
def train(model,train_loader,optimizer,epoch):
    model.train()
    for batch_idx,(data,target) in enumerate(train_loader):
        if args.cuda:
            data,target=data.cuda(),target.cuda()
        data,target=Variable(data),Variable(target)
        optimizer.zero_grad()
        output=model(data)
        loss=F.nll_loss(output,target.long())
        loss.backward()
        optimizer.step()
        if batch_idx % args.log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader), loss.data[0]))
#Test function
def test(model,test_loader):
    model.eval()
    correct=0
    counter=0
    for batch_idx,(data,target) in enumerate(test_loader):
        if args.cuda:
            data,target=data.cuda(),target.cuda()
        output=model(Variable(data))
        _,predicted=torch.max(output.data,1)
        if counter==0:
            trueList=target.long()
            predictedList=predicted.view(len(predicted))
        else:
            trueList=torch.cat((trueList,target.long()),0)
            predictedList=torch.cat((predictedList,predicted.view(len(predicted))),0)
        correct+=(predicted==target.long().view(-1,1)).sum()
        counter=counter+1
    accuracy=100*(correct/len(test_loader.dataset))
    return accuracy,trueList,predictedList
#Converting categorical variables to original labels
def vallabels(valueList,labels):
    convList=[]
    for value in valueList:
        convList.append(labels[value])
    return convList
#Main function
if __name__=="__main__":
    args = parser.parse_args()
    args.cuda = not args.no_cuda and torch.cuda.is_available()
    torch.manual_seed(args.seed)
    if args.cuda:
        torch.cuda.manual_seed(args.seed)
    model=Net()
    if args.cuda:
        model.cuda()
    #optimizer and criterion for neural network    
    optimizer=optim.SGD(model.parameters(),lr=args.lr,momentum=args.momentum)
    #normalizing data
    normalize=transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
    train_transform=transforms.Compose([transforms.RandomSizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            normalize])
    test_transform=transforms.Compose([transforms.RandomSizedCrop(224),
            transforms.ToTensor(),
            normalize])
    validation_transform=transforms.Compose([transforms.RandomSizedCrop(224),
            transforms.ToTensor(),
            normalize])
    #taking directory with image folder from command line
    imagefile=args.data
    labelfile=args.label
    #calling class from imageloading
    imagedataset=Image.Imagedataset(imagefile,labelfile)
    #splitting datasets into train,test and validation datasets
    trainvaliddataset,testdataset=train_test_split(imagedataset,test_size=0.2,random_state=0)
    traindataset,validdataset=train_test_split(trainvaliddataset,test_size=0.2,random_state=0)
    #Transformation of image into tensor
    traindataset=train_transformation(traindataset,train_transform)
    testdataset=test_transformation(testdataset,test_transform)
    validdataset=validation_transformation(validdataset,validation_transform)
    #dataloader for train test and validation
    train_loader=torch.utils.data.DataLoader(traindataset,batch_size=args.batchsize)
    test_loader=torch.utils.data.DataLoader(testdataset,batch_size=args.testbatchsize)
    validation_loader=torch.utils.data.DataLoader(validdataset,batch_size=args.validbatchsize)
    #Training phase
    prev_accuracy=0
    for epoch in range(0,args.epochs):
        train(model,train_loader,optimizer,epoch)
        accuracy,predictedList,trueList=test(model,validation_loader)
        print('Accuracy of the network on the validation images: %d %%' % (accuracy))
        if(prev_accuracy>accuracy):break
        prev_accuracy=accuracy
    accuracy,predictedList,trueList=test(model,test_loader)
    print('Accuracy of network on test images:%d %%' %(accuracy))
    print('ROC Curve')
    labels=['drink','food','inside','menu','outside']
    predlabels=vallabels(predictedList,labels)
    truelabels=vallabels(trueList,labels)
    print('...................Confusion Matrix.........................')
    confusionmatrix=confusion_matrix(truelabels,predlabels,labels=['drink','food','inside','menu','outside'])
    print(confusionmatrix)
    print('...................Precision............................')
    #taking row sumand column sum of confusion matrix
    col_sum=confusion_matrix.sum(axis=0)
    row_sum=confusion_matrix.sum(axis=1)
    precision=dict()
    recall=dict()
    f1score=dict()
    for i in range(5):
        if(col_sum[i]!=0):
            precision[i]=confusion_matrix[i][i]/col_sum[i]
        else:
            precision[i]=0
        if(row_sum[i]!=0):
            recall[i]=confusion_matrix[i][i]/row_sum[i]
        else:
            recall[i]=0
    print(labels)
    print(precision.values())
    print('.....................Recall.............................')
    print(labels)
    print(recall.values())
    print('.....................F1 Measure..........................')
    #converting predicted list and target list into numpy
    trueval=trueList.numpy()
    predictedval=predictedList.numpy()
    macrof1score=f1_score(trueval,predictedval,average='macro')
    print('F1 Score with Macro average:',macrof1score)
    microf1score=f1_score(trueval,predictedval,average='micro')
    print('F1 Score with Micro average:',microf1score)
    weightedf1score=f1_score(trueval,predictedval,average='weighted')
    print('F1 Score with weighted average:',weightedf1score)
    torch.save(model,'training.pt')
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        

    


    
    
        
        