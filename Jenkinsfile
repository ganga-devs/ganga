pipeline {
  agent any
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
  }
  stages {
    // Build stage
    stage('Build Core Image') {
      steps {
        sh label: "Docker build", script: "docker build -t gangacoretest:${env.BRANCH_NAME}-${env.BUILD_ID} -f ${env.WORKSPACE}/ganga/GangaCore/test/Dockerfile ."
      }
    }
    stage('Build Package Images') {
      parallel {
        stage('GangaDirac') {
          environment {
            DIRAC_VERSION = sh(script:  "curl -s https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/release.notes | grep -oP '\[v.*r.*\]' | grep -m1 -v -e "pre" | awk -F"[\[\]]" '{print $2}'", , returnStdout: true).trim()
          }
          steps {
            withCredentials([file(credentialsId: 'GangaRobotUsercert', variable: 'X509_USER_CERT'),
                             file(credentialsId: 'GangaRobotUserkey', variable: 'X509_USER_KEY')]) {
              sh label: "Wait to let GancaCore container register", script: "sleep 120"
              sh label: "Docker build", script: "DOCKER_BUILDKIT=1 docker build -t gangadiractest:${env.BRANCH_NAME}-${env.BUILD_ID} --build-arg VO=gridpp --build-arg BRANCH_NAME=${env.BRANCH_NAME} --build-arg BUILD_ID=${env.BUILD_ID} --build-arg DIRAC_VERSION=${env.DIRAC_VERSION} --secret id=usercert,src=$X509_USER_CERT --secret id=userkey,src=$X509_USER_KEY -f ${env.WORKSPACE}/ganga/GangaDirac/test/Dockerfile ."
            }
          }
        }
        stage('GangaLHCb') {
          steps {
            sh label: "Docker build", script: "docker build -t gangalhcbtest:${env.BRANCH_NAME}-${env.BUILD_ID} -f ${env.WORKSPACE}/ganga/GangaLHCb/test/Dockerfile ."
          }
        }
      }
    }
    // Parallel testing stage
    stage('Run Testing') {
      parallel {
        stage('GangaCore') {
          steps {
            sh label: "Docker run", script: "docker run --name GangaCore${env.BRANCH_NAME}-${env.BUILD_ID} gangacoretest:${env.BRANCH_NAME}-${env.BUILD_ID} || true"
            sh label: "Extract test results", script: "docker cp GangaCore${env.BRANCH_NAME}-${env.BUILD_ID}:/ganga/tests-GangaCore.xml ."
            sh label: "Extract coverage results", script: "docker cp GangaCore${env.BRANCH_NAME}-${env.BUILD_ID}:/ganga/cov-GangaCore.xml ."
          }
          post {
            always {
              sh label: "Force remove container" , script: "docker rm --force GangaCore${env.BRANCH_NAME}-${env.BUILD_ID} || true"
              junit "**/tests-GangaCore.xml"
            }
          }
        }
        stage('GangaDirac') {
          steps {
            sh label: "Docker run", script: "docker run --name GangaDirac${BRANCH_NAME}-${BUILD_ID} gangadiractest:${BRANCH_NAME}-${BUILD_ID} || true"
            sh label: "Extract test results", script: "docker cp GangaDirac${env.BRANCH_NAME}-${env.BUILD_ID}:/ganga/tests-GangaDirac.xml ."
            sh label: "Extract coverage results", script: "docker cp GangaDirac${env.BRANCH_NAME}-${env.BUILD_ID}:/ganga/cov-GangaDirac.xml ."
          }
          post {
            always {
              sh label: "Force remove container", script: "docker rm --force GangaDirac${env.BRANCH_NAME}-${env.BUILD_ID} || true"
              sh label: "Force remove image", script: "docker rmi --force gangadiractest:${env.BRANCH_NAME}-${env.BUILD_ID} || true"
              junit "**/tests-GangaDirac.xml"
            }
          }
        }
        stage('GangaLHCb') {
          steps {
            sh label: "Docker run", script: "docker run --privileged --name GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID} gangalhcbtest:${env.BRANCH_NAME}-${env.BUILD_ID} || true"
            sh label: "Extract test results", script: "docker cp GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID}:/root/tests-GangaLHCb.xml ."
            sh label: "Extract coverage results", script: "docker cp GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID}:/root/cov-GangaLHCb.xml ."
          }
          post {
            always {
              sh label: "Force remove container", script: "docker rm --force GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID} || true"
              sh label: "Force remove image", script: "docker rmi --force gangalhcbtest:${env.BRANCH_NAME}-${env.BUILD_ID} || true"
              junit "**/tests-GangaLHCb.xml"
            }
          }
        }
      } // end parallel
    }
  }
  post { 
    always { 
      cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'cov-*.xml', conditionalCoverageTargets: '70, 0, 0', failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false
      sh "docker rmi --force gangacoretest:${env.BRANCH_NAME}-${env.BUILD_ID}"
      sh "docker system prune -f"
    }
  }
}
