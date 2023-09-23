#!/bin/bash

# Exit on error
set -e

# Install required packages for rootless-docker, json processing, request handling
install_packages() {
    local PACKAGES=(docker.io docker-doc docker-compose podman-docker containerd runc)
    for pkg in "${PACKAGES[@]}"; do
        sudo apt-get remove -y "$pkg"
    done

    sudo apt-get update
    sudo apt-get install -y jq ca-certificates curl gnupg docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin uidmap
}

# Install docker repo
setup_docker() {
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

    local DEB_ARCH=$(dpkg --print-architecture)
    local VERSION_CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
    echo "deb [arch=${DEB_ARCH} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
}

# Create defacto docker account and user process persistence across logouts
setup_user() {
    local USER_NAME="docker-primary"
    if ! id "$USER_NAME" &>/dev/null; then
        adduser --disabled-password --gecos "" $USER_NAME
        echo "$USER_NAME:some-password" | chpasswd
    fi

    loginctl enable-linger docker-primary
}

# Install NVM, Node and npm
setup_nvm_node() {
    local NVM_VERSION="v0.39.5"
    curl -o- "https://raw.githubusercontent.com/nvm-sh/nvm/$NVM_VERSION/install.sh" | bash

    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
    nvm install node
    nvm use node

    npm install -g npm
}

install_packages
setup_docker
setup_user
setup_nvm_node
