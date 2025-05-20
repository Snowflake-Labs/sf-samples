terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=3.0.0"
    }
  }
}

# Remember to:
# * download AzureCLI
# * authenticate in AzureCLI: https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/guides/azure_cli
provider azurerm {
  features {}
}

variable "a_location" {
  type        = string
  description = "Snowflake location (example value: East US 2)"
}

resource "azurerm_resource_group" "snowflake_pl_rg" {
  name     = "snowflake_pl_rg"
  location = var.a_location
}

resource "azurerm_virtual_network" "snowflake_pl_vnet" {
  name                = "snowflake_pl_vpn"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.snowflake_pl_rg.location
  resource_group_name = azurerm_resource_group.snowflake_pl_rg.name
}

resource "azurerm_subnet" "snowflake_pl_subnet" {
  name                 = "snowflake_pl_subnet"
  resource_group_name  = azurerm_resource_group.snowflake_pl_rg.name
  virtual_network_name = azurerm_virtual_network.snowflake_pl_vnet.name
  address_prefixes     = ["10.0.2.0/24"]
  enforce_private_link_service_network_policies = true
}

resource "azurerm_network_security_group" "snowflake_pl_nsg" {
  name                = "snowflake_pl_nsg"
  location            = azurerm_resource_group.snowflake_pl_rg.location
  resource_group_name = azurerm_resource_group.snowflake_pl_rg.name

  security_rule {
    name                       = "Allow-SSH"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_network_interface" "snowflake_pl_ni" {
  name                = "snowflake_pl_ni"
  location            = azurerm_resource_group.snowflake_pl_rg.location
  resource_group_name = azurerm_resource_group.snowflake_pl_rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.snowflake_pl_subnet.id
    private_ip_address_allocation = "Dynamic"

    # Public IP for allowing SSH connection to set up the machine. Remove after the setup.
    public_ip_address_id = azurerm_public_ip.snowflake_pl_public_ip.id
  }
}

resource "azurerm_network_interface_security_group_association" "snowflake_pl_nsg_ni" {
  network_interface_id      = azurerm_network_interface.snowflake_pl_ni.id
  network_security_group_id = azurerm_network_security_group.snowflake_pl_nsg.id
}

resource "azurerm_public_ip" "snowflake_pl_public_ip" {
  name                = "snowflake-pl-public-ip"
  sku                 = "Standard"
  location            = azurerm_resource_group.snowflake_pl_rg.location
  resource_group_name = azurerm_resource_group.snowflake_pl_rg.name
  allocation_method   = "Static"
}

resource "azurerm_linux_virtual_machine" "snowflake_pl_vm" {
  name                = "snowflake-pl-vm"
  resource_group_name = azurerm_resource_group.snowflake_pl_rg.name
  location            = azurerm_resource_group.snowflake_pl_rg.location
  size                = "Standard_B1s"
  admin_username      = "adminuser"
  network_interface_ids = [
    azurerm_network_interface.snowflake_pl_ni.id,
  ]

  admin_ssh_key {
    username   = "adminuser"
    public_key = file("~/.ssh/id_rsa.pub")
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts"
    version   = "latest"
  }
}

resource "azurerm_lb" "snowflake_pl_lb" {
  name                = "snowflake-pl-lb"
  location            = azurerm_resource_group.snowflake_pl_rg.location
  resource_group_name = azurerm_resource_group.snowflake_pl_rg.name
  sku                 = "Standard"

  frontend_ip_configuration {
    name                          = "snowflake_pl_fe_ip"
    subnet_id                     = azurerm_subnet.snowflake_pl_subnet.id
    private_ip_address_allocation = "Dynamic"
  }
}

# LB Backend Pool
resource "azurerm_lb_backend_address_pool" "snowflake_pl_bp" {
  name            = "snowflake_pl_bp"
  loadbalancer_id = azurerm_lb.snowflake_pl_lb.id
}

# LB Probe
resource "azurerm_lb_probe" "snowflake_pl_probe" {
  name                = "snowflake_pl_probe"
  loadbalancer_id     = azurerm_lb.snowflake_pl_lb.id
  protocol            = "Tcp"
  port                = 443
}

# LB Rule
resource "azurerm_lb_rule" "snowflake_pl_lb_443" {
  name                           = "snowflake_pl_lb_443"
  loadbalancer_id                = azurerm_lb.snowflake_pl_lb.id
  protocol                       = "Tcp"
  frontend_port                  = 443
  backend_port                   = 443
  frontend_ip_configuration_name = azurerm_lb.snowflake_pl_lb.frontend_ip_configuration[0].name
  backend_address_pool_ids       = [azurerm_lb_backend_address_pool.snowflake_pl_bp.id]
  probe_id                       = azurerm_lb_probe.snowflake_pl_probe.id
}

# Associate VM NIC to LB Backend Pool
resource "azurerm_network_interface_backend_address_pool_association" "snowflake_pl_vma" {
  network_interface_id    = azurerm_network_interface.snowflake_pl_ni.id
  ip_configuration_name   = azurerm_network_interface.snowflake_pl_ni.ip_configuration[0].name
  backend_address_pool_id = azurerm_lb_backend_address_pool.snowflake_pl_bp.id
}


resource "azurerm_private_link_service" "snowflake_pls" {
  name                = "snowflake_pls"
  location            = azurerm_resource_group.snowflake_pl_rg.location
  resource_group_name = azurerm_resource_group.snowflake_pl_rg.name

  load_balancer_frontend_ip_configuration_ids = [
    azurerm_lb.snowflake_pl_lb.frontend_ip_configuration[0].id
  ]

  nat_ip_configuration {
    name                       = "snowflake_pl_ip_config"
    subnet_id                  = azurerm_subnet.snowflake_pl_subnet.id
    private_ip_address         = "10.0.2.17"
    private_ip_address_version = "IPv4"
    primary                    = true
  }
}