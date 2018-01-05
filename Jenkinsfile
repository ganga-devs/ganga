pipeline {
  agent any
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
  }
  stages {
    // Build stage
    stage('Build Core Image') {
      steps {
        sh "docker build -t gangacoretest:${env.BRANCH_NAME} -f ${env.WORKSPACE}/python/GangaCore/test/Dockerfile ."
      }
    }
    // Parallel testing stage
    stage('Test') {
      parallel {
        stage('GangaCore') {
          steps {
            sh "docker run --name GangaCore${env.BRANCH_NAME} gangacoretest:${env.BRANCH_NAME} /root/ganga/python/GangaCore/test --cov-report term --cov-report xml:cov-GangaCore.xml --cov /root/ganga/python/GangaCore  --junitxml tests-GangaCore.xml || true"
            sh "docker cp GangaCore${env.BRANCH_NAME}:/root/tests-GangaCore.xml ."
            sh "docker cp GangaCore${env.BRANCH_NAME}:/root/cov-GangaCore.xml ."
          }
          post {
            always {
              junit "**/tests-GangaCore.xml"
              sh "docker rm GangaCore${env.BRANCH_NAME}" 
            }
          }
        }
        stage('GangaDirac') {
          steps {
            sh "docker build -t gangadiractest:${env.BRANCH_NAME} -f ${env.WORKSPACE}/python/GangaDirac/test/Dockerfile ."
            sh "docker run --name GangaDirac${env.BRANCH_NAME} -v ~/.globus:/root/.globus -e vo=gridpp gangadiractest:${env.BRANCH_NAME} || true"
            sh "docker cp GangaDirac${env.BRANCH_NAME}:/root/tests-GangaDirac.xml ."
            sh "docker cp GangaDirac${env.BRANCH_NAME}:/root/cov-GangaDirac.xml ."
          }
          post {
            always {
              junit "**/tests-GangaDirac.xml"
              sh "docker rm GangaDirac${env.BRANCH_NAME}"
              sh "docker rmi gangadiractest:${env.BRANCH_NAME}"
            }
          }
        }
        stage('GangaLHCb') {
          steps {
            sh "docker build -t gangalhcbtest:${env.BRANCH_NAME} -f ${env.WORKSPACE}/python/GangaLHCb/test/Dockerfile ."
            sh "docker run --privileged --name GangaLHCb${env.BRANCH_NAME} -v ~/.globus:/root/.globus gangalhcbtest:${env.BRANCH_NAME} || true"
            sh "docker cp GangaLHCb${env.BRANCH_NAME}:/root/tests-GangaLHCb.xml ."
            sh "docker cp GangaLHCb${env.BRANCH_NAME}:/root/cov-GangaLHCb.xml ."
          }
          post {
            always {
              junit "**/tests-GangaLHCb.xml"
              sh "docker rm GangaLHCb${env.BRANCH_NAME}"
              sh "docker rmi gangalhcbtest:${env.BRANCH_NAME}"
            }
          }
        }
      } // end parallel
    }
  }
  post { 
    always { 
      cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'cov-*.xml', conditionalCoverageTargets: '70, 0, 0', failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false
      sh "docker rmi gangacoretest:${env.BRANCH_NAME}"
    }
  }
}
