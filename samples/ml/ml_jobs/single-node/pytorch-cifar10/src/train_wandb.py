import os
import argparse
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
import wandb
from model import Net

def train(args):
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
    ])

    trainset = torchvision.datasets.CIFAR10(root=args.data_dir, train=True, download=False, transform=transform)
    trainloader = DataLoader(trainset, batch_size=args.batch_size, shuffle=True, num_workers=2)

    testset = torchvision.datasets.CIFAR10(root=args.data_dir, train=False, download=False, transform=transform)
    testloader = DataLoader(testset, batch_size=args.batch_size, shuffle=False, num_workers=2)

    wandb.login(key=os.environ['WANDB_API_KEY'])
    service_name = os.environ.get('SNOWFLAKE_SERVICE_NAME', 'test')
    wandb.init(project=f"experiment-{service_name}")

    net = Net().to(device)
    wandb.watch(net)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(net.parameters(), lr=args.learning_rate, momentum=args.momentum)

    writer = SummaryWriter(log_dir=args.tensorboard_dir)

    for epoch in range(args.epochs):
        running_loss = 0.0
        for i, data in enumerate(trainloader, 0):
            inputs, labels = data[0].to(device), data[1].to(device)
            
            optimizer.zero_grad()
            outputs = net(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            
            running_loss += loss.item()
            if i % 200 == 199:
                avg_loss = running_loss / 200
                writer.add_scalar('training loss', avg_loss, epoch * len(trainloader) + i)
                wandb.log({"training loss": avg_loss}, step=epoch * len(trainloader) + i)
                running_loss = 0.0
        
        correct = 0
        total = 0
        with torch.no_grad():
            for data in testloader:
                images, labels = data[0].to(device), data[1].to(device)
                outputs = net(images)
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
        
        accuracy = 100 * correct / total
        writer.add_scalar('test accuracy', accuracy, epoch)
        wandb.log({"test accuracy": accuracy}, step=epoch)
        print(f'Epoch {epoch+1}, Test Accuracy: {accuracy}%')

    print('Finished Training')
    model_path = os.path.join(args.model_dir, 'cifar_net.pth')
    torch.save(net.state_dict(), model_path)
    wandb.save(model_path)
    wandb.finish()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CIFAR-10 training script')
    parser.add_argument('--batch-size', type=int, default=64, help='input batch size for training (default: 64)')
    parser.add_argument('--epochs', type=int, default=10, help='number of epochs to train (default: 10)')
    parser.add_argument('--learning-rate', type=float, default=0.001, help='learning rate (default: 0.001)')
    parser.add_argument('--momentum', type=float, default=0.9, help='SGD momentum (default: 0.9)')
    parser.add_argument('--data-dir', type=str, default='/data', help='Data directory')
    parser.add_argument('--model-dir', type=str, default='/model', help='Model/checkpoint save directory')
    parser.add_argument('--tensorboard-dir', type=str, default='/tensorboard', help='Tensorboard log directory')
    args = parser.parse_args()
    
    train(args)