# -*- coding: utf-8 -*-
"""
Created on Sat Nov 18 21:34:09 2017

@author: Sreenivas
"""
from PIL import Image
import os
import os.path
import pandas as pd
import torch.utils.data as data
def pil_loader(path):
    # open path as file to avoid ResourceWarning (https://github.com/python-pillow/Pillow/issues/835)
        with open(path, 'rb') as f:
            with Image.open(f) as img:
                return img.convert('RGB')
def accimage_loader(path):
    import accimage
    try:
        return accimage.Image(path)
    except IOError:
        return pil_loader(path)
def default_loader(path):
    from torchvision import get_image_backend
    if get_image_backend() == 'accimage':
        return accimage_loader(path)
    else:
        return pil_loader(path)
class Imagedataset(data.Dataset):
    #Init function taking csv files for labels and root directory for images
    def __init__(self,root_dir,csv_file,transform=None,loader=default_loader):
        self.labelfile=pd.read_csv(csv_file)
        self.root_dir=root_dir
        self.loader=loader
        self.transform=transform
    #Length of dataset
    def __len__(self):
        return len(self.labelfile)
    #forming tuple of image and its label
    def __getitem__(self,idx):
        img_name=os.path.join(self.root_dir,self.labelfile.ix[idx,0])
        image=self.loader(img_name)
        if self.transform:
            image=self.transform(image)
        return image
    
