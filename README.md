### Setting up the environment:

Create and start up a venv:

<pre>
python3 -m venv venv
source venv/bin/activate
</pre>

Then install all the necessary libraries:

<pre>
pip install -r requirements.txt
</pre>

<pre>
cd script
python3 fetch.py
</pre>

Data returned by the API will be saved in `sout_rainy_days_summary_1.csv`

When finished, deactivate the venv:

<pre>
deactivate
</pre>
