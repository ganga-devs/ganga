echo "Uninstalling" %{name}
echo "Deleting" $RPM_INSTALL_PREFIX/python/%{name}
rm -rf $RPM_INSTALL_PREFIX/python/%{name}

if [ ! "$(ls -A $RPM_INSTALL_PREFIX/python)" ]; then
  echo "$RPM_INSTALL_PREFIX/python is empty...removing."
  rm -rf $RPM_INSTALL_PREFIX/python
fi

if [ ! "$(ls -A $RPM_INSTALL_PREFIX)" ]; then
  echo "$RPM_INSTALL_PREFIX is empty...removing."
  rm -rf $RPM_INSTALL_PREFIX
fi

