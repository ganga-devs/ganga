if [ ! "$(ls -A $RPM_INSTALL_PREFIX)" ]; then
  rm -rf $RPM_INSTALL_PREFIX
  echo $RPM_INSTALL_PREFIX "is empty...removing."
fi

