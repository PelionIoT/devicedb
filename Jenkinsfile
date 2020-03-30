import hudson.Util;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

projectName = 'DeviceDB'
//branchName = "${env.GIT_BRANCH}"

def notifySlack(String buildStatus = 'UNSTABLE') {
    branchName = "${env.BRANCH_NAME}"
    print branchName
    buildStatus = buildStatus ?: 'SUCCESSFUL' 
    def colorCode = '#FF0000' 
 
    if (buildStatus == 'UNSTABLE') { 
        //colorName = 'YELLOW' 
        colorCode = '#FFFF00' 
    } else if (buildStatus == 'SUCCESS') { 
        //colorName = 'GREEN' 
        colorCode = '#00FF00' 
    } else { 
        //colorName = 'RED' 
        colorCode = '#FF0000' 
    } 
 
    def mainText = '*Name -> <' + env.RUN_DISPLAY_URL + '|' + projectName.toString() + '>*'
 
    JSONArray attachments = new JSONArray();
    JSONObject detailsAttachment = new JSONObject(); 

 
    // Create details field for attachment.  
    JSONArray fields = new JSONArray(); 
    JSONObject fieldsObject = new JSONObject(); 

    JSONObject branch = new JSONObject();
    fieldsObject.put('title', 'Branch');
    fieldsObject.put('value', branchName.toString());
    fieldsObject.put('short', true);

    fields.add(fieldsObject); 

    fieldsObject.put('title','Status'); 
    fieldsObject.put('value',buildStatus.toString()); 
    fieldsObject.put('short',true); 
    
    fields.add(fieldsObject); 
    
    fieldsObject = new JSONObject(); 
    fieldsObject.put('title','Job ID'); 
    fieldsObject.put('value','#' + env.BUILD_NUMBER.toString()); 
    fieldsObject.put('short',true); 
    
    fields.add(fieldsObject); 
    
    // Put fields JSONArray 
    detailsAttachment.put('pretext',"Ran DeviceDB CI pipeline on Jenkins"); 
    detailsAttachment.put('title',"Sonarqube Dashboard");
    detailsAttachment.put('title_link',"http://pe-jm.usa.arm.com:9000/dashboard?id=edge%3Adevicedb%3A"+"${branchName}"); 
    //detailsAttachment.put('author_name',"LAVA Job");
    //detailsAttachment.put('author_link',"http://lava.mbedcloudtesting.com/scheduler/alljobs");
    detailsAttachment.put('text',"Click to view Sonarqube Dashboard");
    detailsAttachment.put('fields',fields); 
    detailsAttachment.put('color', colorCode.toString());
    detailsAttachment.put('footer','After ' + Util.getTimeSpanString(System.currentTimeMillis() - currentBuild.startTimeInMillis)) 
    
    attachments.add(detailsAttachment); 

    print detailsAttachment
    
    // Send notifications 
    slackSend (message: mainText.toString(), attachments: attachments.toString()) 
    
}


pipeline {
  agent none
  options{
    skipDefaultCheckout()
  }
  stages {
    stage('Build') {
        agent{
            label 'noi-linux-ubuntu16-ci-slave'
        }
        options{
            lock resource: 'noi-linux-ubuntu16-ci-slave'
        }
        steps {
         //checkout scm
          withEnv(["GOROOT=/home/jenkins/go", "GOPATH=/home/jenkins/goprojects", "PATH+GO=/home/jenkins/goprojects/bin:/home/jenkins/go/bin"]){
              script{
                  sh 'rm -rf /home/jenknis/goprojects/src/github.com/armPelionEdge/devicedb'
                  if(env.BRANCH_NAME ==~ /^PR-[0-9]*$/){
                    sh "go get -u github.com/armPelionEdge/devicedb && cd /home/jenkins/goprojects/src/github.com/armPelionEdge/devicedb && git fetch --all && git checkout ${CHANGE_BRANCH} && git merge origin/${env.CHANGE_TARGET} && go build"
                  }
                  else{
                    sh "go get -u github.com/armPelionEdge/devicedb && cd /home/jenkins/goprojects/src/github.com/armPelionEdge/devicedb && git fetch --all && git checkout ${env.BRANCH_NAME} && git merge origin/${env.BRANCH_NAME} && go build"
                  }
              }
          }
       }
    }
    
    stage('Test'){
      agent{
        label 'noi-linux-ubuntu16-ci-slave'
      }
      steps {
        withEnv(["GOROOT=$HOME/go", "GOPATH=$HOME/goprojects", "PATH+GO=$HOME/goprojects/bin:$HOME/go/bin"]){
          sh 'go get -u github.com/jstemmer/go-junit-report'
          sh "cd /home/jenkins/goprojects/src/github.com/armPelionEdge/devicedb && go test -v -coverpkg=all -coverprofile cov.out 2>&1 ./... | go-junit-report > report.xml && cp report.xml cov.out /home/jenkins/workspace/devicedb_${env.BRANCH_NAME}/"
          sh "cd /home/jenkins/goprojects/src/github.com/armPelionEdge/devicedb && gocover-cobertura < cov.out > coverage.xml && cp coverage.xml /home/jenkins/workspace/devicedb_${env.BRANCH_NAME}/"
          stash includes: 'cov.out', name: 'sonar-coverage'
        }
      }
    }
        
    stage('SonarQube'){
      agent{
        label 'master'
      }
      options{
        checkoutToSubdirectory('/var/jenkins_home/goprojects/src/github.com/armPelionEdge/devicedb')
      }
      environment {
        scannerHome = tool 'SonarQubeScanner'
      }
      steps {
        withSonarQubeEnv('sonarqube') {
          unstash 'sonar-coverage'
          sh "cd $JENKINS_HOME/goprojects/src/github.com/armPelionEdge/devicedb && ${scannerHome}/bin/sonar-scanner && cp -r .scannerwork $JENKINS_HOME/workspace/devicedb_${env.BRANCH_NAME}"
        }
      }
    }
    
    stage('Auto Doc') {
      agent{
        label 'noi-linux-ubuntu16-ci-slave'
      }
      steps {
        withEnv(["GOROOT=$HOME/go", "GOPATH=$HOME/goprojects", "PATH+GO=$HOME/goprojects/bin:$HOME/go/bin"]){
          sh 'go get github.com/robertkrimen/godocdown/godocdown'
          sh 'cd /home/jenkins/goprojects/src/github.com/armPelionEdge/devicedb && go list ./... > devicedb_packages.txt'
          sh 'cd /home/jenkins/goprojects/src/github.com/armPelionEdge/devicedb && input=devicedb_packages.txt && while IFS= read -r line; do godocdown -plain=false $line >> devicedb_docs.md; done < $input'
          sh "cd /home/jenkins/goprojects/src/github.com/armPelionEdge/devicedb && cp devicedb_docs.md /home/jenkins/workspace/devicedb_${env.BRANCH_NAME}/"
        }
      }
    }
  }
  
  post{
    always{
      node('noi-linux-ubuntu16-ci-slave'){
        junit 'report.xml'
        step([$class: 'CoberturaPublisher', autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'coverage.xml', failUnhealthy: false, failUnstable: false, maxNumberOfBuilds: 0, onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false])
        archiveArtifacts artifacts: 'devicedb_docs.md'
        notifySlack("${currentBuild.currentResult}")
        sh 'rm -rf /home/jenknis/goprojects/src/github.com/armPelionEdge/devicedb'
      }
    }
  }
}
