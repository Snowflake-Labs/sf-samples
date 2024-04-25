from torchvision import datasets

class MyImageFolder(datasets.ImageFolder):
    """extends torchvision datasets.ImageFolder class's __getitem__ method to also return image path"""
    def __getitem__(self, index):
        img, label = super(MyImageFolder, self).__getitem__(index)
        path, _ = self.imgs[index]
        return img, label, path