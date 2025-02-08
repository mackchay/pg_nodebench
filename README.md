# pg_nodebench

<div>
  <button onclick="showSection('english')">English</button>
  <button onclick="showSection('russian')">Русский</button>
</div>
<div id="english">
Hello baby
</div>

<div id="russian">
Привет малышка
</div>


<script> function showSection(lang) { document.getElementById('english').style.display = (lang === 'english') 
? 'block' : 'none'; 
document.getElementById('russian').style.display = (lang === 'russian') ? 'block' : 'none'; } 
</script>