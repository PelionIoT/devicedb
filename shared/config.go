package shared

import (
    "crypto/tls"
    "io/ioutil"
    "errors"
    "fmt"
    "gopkg.in/yaml.v2"
    "path/filepath"

    . "devicedb/logging"
)

type YAMLServerConfig struct {
    DBFile string `yaml:"db"`
    Port int `yaml:"port"`
    MaxSyncSessions int `yaml:"syncSessionLimit"`
    SyncSessionPeriod uint64 `yaml:"syncSessionPeriod"`
    SyncPushBroadcastLimit uint64 `yaml:"syncPushBroadcastLimit"`
    SyncExplorationPathLimit uint32 `yaml:"syncExplorationPathLimit"`
    GCInterval uint64 `yaml:"gcInterval"`
    GCPurgeAge uint64 `yaml:"gcPurgeAge"`
    MerkleDepth uint8 `yaml:"merkleDepth"`
    Peers []YAMLPeer `yaml:"peers"`
    TLS YAMLTLSFiles `yaml:"tls"`
    LogLevel string `yaml:"logLevel"`
    Cloud *YAMLCloud `yaml:"cloud"`
    History *YAMLHistory `yaml:"history"`
}

type YAMLHistory struct {
    PurgeOnForward bool `yaml:"purgeOnForward"`
    EventLimit uint64 `yaml:"eventLimit"`
}

type YAMLPeer struct {
    ID string `yaml:"id"`
    Host string `yaml:"host"`
    Port int `yaml:"port"`
}

type YAMLCloud struct {
    ID string `yaml:"id"`
    Host string `yaml:"host"`
    Port int `yaml:"port"`
    NoValidate bool `yaml:"noValidate"`
}

type YAMLTLSFiles struct {
    ClientCertificate string `yaml:"clientCertificate"`
    ClientKey string `yaml:"clientKey"`
    ServerCertificate string `yaml:"serverCertificate"`
    ServerKey string `yaml:"serverKey"`
    Certificate string `yaml:"certificate"`
    Key string `yaml:"key"`
    RootCA string `yaml:"rootCA"`
}

func (ysc *YAMLServerConfig) LoadFromFile(file string) error {
    rawConfig, err := ioutil.ReadFile(file)
    
    if err != nil {
        return err
    }
    
    err = yaml.Unmarshal(rawConfig, ysc)
    
    if err != nil {
        return err
    }
    
    if !isValidPort(ysc.Port) {
        return errors.New(fmt.Sprintf("%d is an invalid port for the database server", ysc.Port))
    }
    
    if ysc.MerkleDepth < MerkleMinDepth || ysc.MerkleDepth > MerkleMaxDepth {
        return errors.New(fmt.Sprintf("Invalid merkle depth specified. Valid ranges are from %d to %d inclusive", MerkleMinDepth, MerkleMaxDepth))
    }
    
    if ysc.MaxSyncSessions <= 0 {
        return errors.New("syncSessionLimit must be at least 1")
    }
    
    if ysc.SyncSessionPeriod == 0 {
        return errors.New("syncSessionPeriod must be positive")
    }

    if ysc.Peers != nil {
        for _, peer := range ysc.Peers {
            if len(peer.ID) == 0 {
                return errors.New(fmt.Sprintf("Peer ID is empty"))
            }
            
            if len(peer.Host) == 0 {
                return errors.New(fmt.Sprintf("The host name is empty for peer %s", peer.ID))
            }
            
            if !isValidPort(peer.Port) {
                return errors.New(fmt.Sprintf("%d is an invalid port to connect to peer %s at %s", peer.Port, peer.ID, peer.Host))
            }
        }
    }
    
    if ysc.Cloud != nil {
        if len(ysc.Cloud.Host) == 0 {
            return errors.New(fmt.Sprintf("The host name is empty for the cloud"))
        }
        
        if !isValidPort(ysc.Cloud.Port) {
            return errors.New(fmt.Sprintf("%d is an invalid port to connect to the cloud at %s", ysc.Cloud.Port, ysc.Cloud.Host))
        }
    }
    
    if ysc.History == nil {
        ysc.History = &YAMLHistory{ }
    }
    
    if len(ysc.TLS.ClientCertificate) == 0 {
        ysc.TLS.ClientCertificate = ysc.TLS.Certificate
    }
    
    if len(ysc.TLS.ServerCertificate) == 0 {
        ysc.TLS.ServerCertificate = ysc.TLS.Certificate
    }
    
    if len(ysc.TLS.ClientKey) == 0 {
        ysc.TLS.ClientKey = ysc.TLS.Key
    }
    
    if len(ysc.TLS.ServerKey) == 0 {
        ysc.TLS.ServerKey = ysc.TLS.Key
    }
    
    clientCertificate, err := ioutil.ReadFile(resolveFilePath(file, ysc.TLS.ClientCertificate))
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load client certificate from %s", ysc.TLS.ClientCertificate))
    }
    
    clientKey, err := ioutil.ReadFile(resolveFilePath(file, ysc.TLS.ClientKey))
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load client key from %s", ysc.TLS.ClientKey))
    }
    
    serverCertificate, err := ioutil.ReadFile(resolveFilePath(file, ysc.TLS.ServerCertificate))
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load server certificate from %s", ysc.TLS.ServerCertificate))
    }
    
    serverKey, err := ioutil.ReadFile(resolveFilePath(file, ysc.TLS.ServerKey))
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load server key from %s", ysc.TLS.ServerKey))
    }
    
    rootCA, err := ioutil.ReadFile(resolveFilePath(file, ysc.TLS.RootCA))
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load root CA chain from %s", ysc.TLS.RootCA))
    }
    
    ysc.TLS.ClientCertificate = string(clientCertificate)
    ysc.TLS.ClientKey = string(clientKey)
    ysc.TLS.ServerCertificate = string(serverCertificate)
    ysc.TLS.ServerKey = string(serverKey)
    ysc.TLS.RootCA = string(rootCA)
    
    _, err = tls.X509KeyPair([]byte(ysc.TLS.ClientCertificate), []byte(ysc.TLS.ClientKey))
    
    if err != nil {
        return errors.New("The specified client certificate and key represent an invalid public/private key pair")
    }
    
    _, err = tls.X509KeyPair([]byte(ysc.TLS.ServerCertificate), []byte(ysc.TLS.ServerKey))
    
    if err != nil {
        return errors.New("The specified server certificate and key represent an invalid public/private key pair")
    }
    
    // purge age must be at least ten minutes
    if ysc.GCPurgeAge < 600000 {
        return errors.New("The gc purge age must be at least ten minutes (i.e. gcPurgeAge: 600000)")
    }
    
    if ysc.GCInterval < 300000 {
        return errors.New("The gc interval must be at least five minutes (i.e. gcInterval: 300000)")
    }

    if ysc.SyncExplorationPathLimit == 0 {
        ysc.SyncExplorationPathLimit = 1000
    }
    
    SetLoggingLevel(ysc.LogLevel)
    
    return nil
}

func isValidPort(p int) bool {
    return p >= 0 && p < (1 << 16)
}

func resolveFilePath(configFileLocation, file string) string {
    if filepath.IsAbs(file) {
        return file
    }
    
    return filepath.Join(filepath.Dir(configFileLocation), file)
}