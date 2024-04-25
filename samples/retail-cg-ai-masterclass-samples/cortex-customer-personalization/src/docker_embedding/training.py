#!/opt/conda/bin/python3
# imports
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim import lr_scheduler
import torch.backends.cudnn as cudnn
import numpy as np
from torchvision import transforms, models
from torchvision.models._utils import IntermediateLayerGetter
from torch.utils.data import Dataset
from PIL import Image
import time
import os
import copy
import tqdm

# get paths from env
img_path = os.getenv("IMAGE_PATH")
anno_path = os.getenv("ANNOTATION_PATH")
save_path = os.getenv("SAVE_PATH")

# create a class to integer mapping, training requires integer labels
classes = pd.read_csv(os.path.join(anno_path, 'sim_train.csv')).label.unique()
classes.sort()
class_mapping = {c: i for i, c in enumerate(classes)}

# set device, will automatically select cuda/mps is GPUs/apple silicone are available
if not torch.backends.mps.is_available():
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
else:
    device = torch.device("mps")
    
# Data augmentation and normalization for training
# Just normalization for validation
data_transforms = {
    'train': transforms.Compose([
        transforms.Resize(256),
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ]),
    'val': transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ]),
}

# custom dataset class, builds dataset from provided annotation file
class CustomImageDataset(Dataset):
    def __init__(self, annotations_file, img_dir, transform=None, target_transform=None):
        self.img_labels = pd.read_csv(annotations_file)
        self.classes = list(class_mapping.keys())
        self.img_dir = img_dir
        self.transform = transform
        self.target_transform = target_transform

    def __len__(self):
        return len(self.img_labels)

    def __getitem__(self, idx):
        img_path = os.path.join(self.img_dir, self.img_labels.iloc[idx, 0])
        image = Image.open(img_path)
        label = self.img_labels.iloc[idx, 1]
        label = class_mapping[label] # map string label to integer
        if self.transform:
            image = self.transform(image)
        if self.target_transform:
            label = self.target_transform(label)
        return image, label
    
# Model Training Loop
def train_model(model, criterion, optimizer, scheduler, num_epochs=25):
    since = time.time()

    best_model_wts = copy.deepcopy(model.state_dict())
    best_acc = 0.0

    for epoch in range(num_epochs):
        print(f'Epoch {epoch}/{num_epochs - 1}')
        print('-' * 10)

        # Each epoch has a training and validation phase
        for phase in ['train', 'val']:
            if phase == 'train':
                model.train()  # Set model to training mode
            else:
                model.eval()   # Set model to evaluate mode

            running_loss = 0.0
            running_corrects = 0

            # Iterate over data.
            for inputs, labels in tqdm.tqdm(dataloaders[phase]):
                inputs = inputs.to(device)
                labels = labels.to(device)

                # zero the parameter gradients
                optimizer.zero_grad()

                # forward
                # track history if only in train
                with torch.set_grad_enabled(phase == 'train'):
                    outputs = model(inputs)
                    _, preds = torch.max(outputs, 1)
                    loss = criterion(outputs, labels)

                    # backward + optimize only if in training phase
                    if phase == 'train':
                        loss.backward()
                        optimizer.step()

                # statistics
                running_loss += loss.item() * inputs.size(0)
                running_corrects += torch.sum(preds == labels.data)
            if phase == 'train':
                scheduler.step()

            epoch_loss = running_loss / dataset_sizes[phase]
            epoch_acc = running_corrects.float() / dataset_sizes[phase]

            print(f'{phase} Loss: {epoch_loss:.4f} Acc: {epoch_acc:.4f}')

            # deep copy the model
            if phase == 'val' and epoch_acc > best_acc:
                best_acc = epoch_acc
                best_model_wts = copy.deepcopy(model.state_dict())

        print()

    time_elapsed = time.time() - since
    print(f'Training complete in {time_elapsed // 60:.0f}m {time_elapsed % 60:.0f}s')
    print(f'Best val Acc: {best_acc:4f}')

    # load best model weights
    model.load_state_dict(best_model_wts)
    return model

# create datasets/dataloaders for train and val using the appropriate annotation file and transform
image_datasets = {
    'train': CustomImageDataset(
        annotations_file = os.path.join(anno_path, 'sim_train.csv'),
        img_dir = img_path,
        transform = data_transforms['train']
    ),
    'val': CustomImageDataset(
        annotations_file = os.path.join(anno_path, 'sim_test.csv'),
        img_dir = img_path,
        transform = data_transforms['val']
    ),    
}

# create dataloaders for train and val with specified batch size
dataloaders = {
    x: torch.utils.data.DataLoader(
        image_datasets[x], 
        batch_size=16,
        shuffle=True, 
        num_workers=4)
    for x in ['train', 'val']
}

# get dataset sizes and class names, used in some computations later
dataset_sizes = {x: len(image_datasets[x]) for x in ['train', 'val']}
class_names = image_datasets['train'].classes

# initialize model with pretrained weights
model_ft = models.resnet18(weights='IMAGENET1K_V1')
num_ftrs = model_ft.fc.in_features
# add classification head for fine tuning and move to device
model_ft.fc = nn.Linear(num_ftrs, len(class_names))
model_ft = model_ft.to(device)

# set loss function
criterion = nn.CrossEntropyLoss()

# Observe that all parameters are being optimized
optimizer_ft = optim.SGD(model_ft.parameters(), lr=0.001, momentum=0.9)

# Decay LR by a factor of 0.1 every 7 epochs
exp_lr_scheduler = lr_scheduler.StepLR(optimizer_ft, step_size=7, gamma=0.1)

# start training
model_ft = train_model(model_ft, criterion, optimizer_ft, exp_lr_scheduler, num_epochs=25)

# save best model
torch.save(model_ft, os.path.join(save_path, 'best_model.pt'))

# create embedding model
new_m = IntermediateLayerGetter(model_ft, {'avgpool': 'features'})
new_m = new_m.to('cpu')

# save embedding model
torch.save(new_m, os.path.join(save_path, 'embedding_model.pt'))

print(f'Saved Models\nBest Model: os.path.join(save_path, 'best_model.pt')\nEmbedding Model:{save_path}/{embedding_model.pt}')