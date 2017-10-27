# GangaDirac Package
---

This package allows **Ganga** to submit and manage jobs on a **DIRAC** based system.
To use this package follow the following steps.
**Note:** If you already have a **DIRAC UI** or **pip** installed then you can skip ahead.

### Setup
 - Install a **DIRAC** user interface (**UI**). For GridPP users you can follow the installation instructions on the GridPP Wiki.
   [https://www.gridpp.ac.uk/wiki/Quick_Guide_to_Dirac](https://www.gridpp.ac.uk/wiki/Quick_Guide_to_Dirac#Dirac_client_installation)
 - Install Python package manager **pip**.
   ```bash
   %> curl https://bootstrap.pypa.io/get-pip.py | python - --user
   ```
   **Note:** I assume here that you are not root and therefore by parsing the `--user` flag we install it in the user's local area.

   ---
   #### Optional
 - Install and activate Python **virtual environment**.
   ```bash
   %> pip install --user virtualenv
   %> virtualenv venv
   %> . venv/bin/activate
   ```
   **Note:** I make the same assumption that you are not root and hence use the `--user` flag to install in the user's local area.

   ---
 - Install **Ganga** in whichever way you prefer. We recommend using `pip` to install from **PyPI**.
   ```bash
   (venv) %> pip install ganga
   ```
   **Note:** If you did **not** setup a virtual environment in the above optional step **and** do **not** have root privileges, you will need to add the `--user` flag to install in the user's local area as below.
   ```bash
   %> pip install --user ganga
   ```
 - Generate the **Ganga** configuration file complete with the necessary **DIRAC** extensions.
   ```bash
   %> ganga -o[Configuration]RUNTIME_PATH=GangaDirac -g
   ```
 - Add the following options to your `~/.gangarc` file.
   ```ini
   [Configuration]
   RUNTIME_PATH = GangaDirac

   [DIRAC]
   DiracEnvSource = <path to the DIRAC UI bashrc file>

   [defaults_DiracProxy]
   group = <your DIRAC group>  # Note this is typically <your VO>_user. e.g. gridpp_user
   ```
---
### Usage
 - Start **Ganga**.
   ```bash
   %> ganga
   ```
 - Create and submit a **DIRAC** test job.
   ```python
   j = Job()
   j.backend = Dirac()
   j.submit()
   ```
   **Note:** This can also be done inline:
   ```python
   Job(backend=Dirac()).submit()
   ```
