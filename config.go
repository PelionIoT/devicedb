package devicedb

import (
    "crypto/tls"
    "encoding/json"
    "io/ioutil"
    "errors"
    "fmt"
)

type JSONServerConfig struct {
    DBFile string `json:"db"`
    Port int `json:"port"`
    MaxSyncSessions int `json:"syncSessionLimit"`
    MerkleDepth uint8 `json:"merkleDepth"`
    Peers []JSONPeer `json:"peers"`
    TLS JSONTLSFiles `json:"tls"`
}

type JSONPeer struct {
    ID string `json:"id"`
    Host string `json:"host"`
    Port int `json:"port"`
}

type JSONTLSFiles struct {
    ClientCertificate string `json:"clientCertificate"`
    ClientKey string `json:"clientKey"`
    ServerCertificate string `json:"serverCertificate"`
    ServerKey string `json:"serverKey"`
    RootCA string `json:"rootCA"`
}

func isValidPort(p int) bool {
    return p >= 0 && p < (1 << 16)
}

func (jsc *JSONServerConfig) LoadFromFile(file string) error {
    rawConfig, err := ioutil.ReadFile(file)
    
    if err != nil {
        return err
    }
    
    err = json.Unmarshal(rawConfig, jsc)
    
    if err != nil {
        return err
    }
    
    if !isValidPort(jsc.Port) {
        return errors.New(fmt.Sprintf("%d is an invalid port for the database server", jsc.Port))
    }
    
    if jsc.MerkleDepth < MerkleMinDepth || jsc.MerkleDepth > MerkleMaxDepth {
        return errors.New(fmt.Sprintf("Invalid merkle depth specified. Valid ranges are from %d to %d inclusive", MerkleMinDepth, MerkleMaxDepth))
    }
    
    if jsc.MaxSyncSessions <= 0 {
        return errors.New("syncSessionLimit must be at least 1")
    }

    if jsc.Peers != nil {
        for _, peer := range jsc.Peers {
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
    
    clientCertificate, err := ioutil.ReadFile(jsc.TLS.ClientCertificate)
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load client certificate from %s", jsc.TLS.ClientCertificate))
    }
    
    clientKey, err := ioutil.ReadFile(jsc.TLS.ClientKey)
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load client key from %s", jsc.TLS.ClientKey))
    }
    
    serverCertificate, err := ioutil.ReadFile(jsc.TLS.ServerCertificate)
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load server certificate from %s", jsc.TLS.ServerCertificate))
    }
    
    serverKey, err := ioutil.ReadFile(jsc.TLS.ServerKey)
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load server key from %s", jsc.TLS.ServerKey))
    }
    
    rootCA, err := ioutil.ReadFile(jsc.TLS.RootCA)
    
    if err != nil {
        return errors.New(fmt.Sprintf("Could not load root CA chain from %s", jsc.TLS.RootCA))
    }
    
    jsc.TLS.ClientCertificate = string(clientCertificate)
    jsc.TLS.ClientKey = string(clientKey)
    jsc.TLS.ServerCertificate = string(serverCertificate)
    jsc.TLS.ServerKey = string(serverKey)
    jsc.TLS.RootCA = string(rootCA)
    
    _, err = tls.X509KeyPair([]byte(jsc.TLS.ClientCertificate), []byte(jsc.TLS.ClientKey))
    
    if err != nil {
        return errors.New("The specified client certificate and key represent an invalid public/private key pair")
    }
    
    _, err = tls.X509KeyPair([]byte(jsc.TLS.ServerCertificate), []byte(jsc.TLS.ServerKey))
    
    if err != nil {
        return errors.New("The specified server certificate and key represent an invalid public/private key pair")
    }
    
    return nil
}