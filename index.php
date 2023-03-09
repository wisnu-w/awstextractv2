<!DOCTYPE html>
<html lang="en" style="--bs-body-bg: 161e2d;">


<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, shrink-to-fit=no">
    <title>AWS Textract Demo</title>
    <link rel="stylesheet" href="assets/bootstrap/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Nunito+Sans:wght@400;600&amp;display=swap">
    <script src="https://code.jquery.com/jquery-3.6.1.min.js" integrity="sha256-o88AwQnZB+VDvE9tvIXrMQaPlFFSUTR+nldQm1LuPXQ=" crossorigin="anonymous"></script>
    <script>

        function eraseText() {
            $("#resultTA").text('');
        }
        function addData(haslo) {
           
          document.getElementById("resultTA").innerHTML = haslo;   
            
        }
        $(document).ready(function name() {
           $("#file").click(function name() {
                eraseText();
           })
        });
        function prettyPrint() {
          var badJSON = document.getElementById('resultTA').value;
          var parseJSON = JSON.parse(badJSON);
          var JSONInPrettyFormat = JSON.stringify(parseJSON, undefined, 4);
          document.getElementById('resultTA').innerHTML = JSONInPrettyFormat;
        }
    </script>
</head>

<body class="flex-shrink-0" style="--bs-body-bg: #f2f3f3;">
    <div id="tempDiv" style="display: none;"></div>
    <nav class="navbar navbar-light navbar-expand-md" style="background-color: #dad7d0;">
        <div class="container-fluid "><img src="assets/img/vibilogo.png" width="80" height="80" style="margin: 10px;"><button data-bs-toggle="collapse" class="navbar-toggler" data-bs-target="#navcol-1"><span class="visually-hidden">Toggle navigation</span><span class="navbar-toggler-icon"></span></button>
            <div class="collapse navbar-collapse" id="navcol-1" >
                <ul class="navbar-nav" style="color: #ffffff;">
                    <li class="nav-item"><a class="nav-link active" href="#" style="color: #3997c8; font-size: 40px;">AWS TEXTRACT</a></li>
                    <li class="nav-item"></li>
                    <li class="nav-item"></li>
                    <li class="nav-item"></li>
                </ul><a class="btn btn-primary ms-auto" role="button" href="https://docs.aws.amazon.com/textract/latest/dg/what-is.html" target="_blank">Docs</a>
            </div>
        </div>
    </nav>
    <div class="container ">
        <h1 class="text-start mx-auto" style="margin-top:45px; margin-bottom:35px; color: #1a1a1a;">PLEASE DO NOT SPAM</h1>
        <p style="color: #1a1a1a;text-align: justify;">Amazon Textract is a machine learning (ML) service that automatically extracts text, handwriting, and data from scanned documents. It goes beyond simple optical character recognition (OCR) to identify, understand, and extract data from forms and tables</p>
    </div>
    <div class="container " style="margin-top: 40px;">
    <form action="index.php" method="post" enctype="multipart/form-data" name="form1" id="form1">
        <div class="row">
            <div class="col-md-6">
                <div class="card d-flex flex-row align-items-center align-self-center m-auto rounded shadow" style="margin-left: 40px;height: 100%;width: 100%;">
                    <div class="card-body justify-content-center align-items-center" style="text-align: center;">
                        <h4 class="card-title" style="text-align: center;">Step 1</h4>
                        <p class="card-text" style="text-align: center;">Upload your image</p>
                        <p><input type="file" name="photo" id="file" accept=".pdf, image/*" style="color: #000000;text-align: center;margin: 8px; width: 41%;" onchange="return fileValidation(file)"></p>
                        <p>Choose data outputs: <br>
                        <input type="radio" id="html" name="outputs" value="raw">
                        <label for="html">Raw Text</label>
                        <input type="radio" id="css" name="outputs" value="form">
                        <label for="css">Forms</label>
                        <input type="radio" id="javascript" name="outputs" value="tables">
                        <label for="javascript">Tables</label></p>
                        
                        <p class="submit">
                          <input type="submit" name="Submit" id="tea" value="Upload image" /> <br />
                        </p>
                        </p>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card d-flex flex-row align-items-center align-self-center m-auto rounded shadow " style="margin-left: 40px;width: 100%;height: 100%;">
                    <div class="card-body">
                        <h4 class="card-title" style="text-align: center;">Result</h4>
                        <div id="response"></div>
                        <p class="card-text" style="text-align: center;">Your result will be seen here</p>
                        <button onclick="prettyPrint()">Pretty Print</button>
                        <textarea cols=40 rows=5 class="form-control-lg d-flex mx-auto" style="text-align: center;" readonly="" id="resultTA"></textarea>
                    </div>
                </div>
            </div>
            <div class="col-md-12">
           <iframe title="w" width="1024" height="760" src="https://app.powerbi.com/view?r=eyJrIjoiYTFiZDdmOGYtMGExYy00OGJmLWFiMTctNTY2MjQxNTdmNTg4IiwidCI6Ijc2MTIxOTIxLTIwNGUtNDE4OC05NDVjLWYwZDA1YmU1NWUwZiIsImMiOjEwfQ%3D%3D" frameborder="0" allowFullScreen="true"></iframe> 
            </div>
          </div>
    </form>  
    </div>
    <script src="assets/bootstrap/js/bootstrap.min.js"></script>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.3/jquery.min.js"></script>
</body>

</html>
<?php

error_reporting(0);
if(isset($_POST['Submit'])!= NULL)
  {
    $file_name = $_FILES['photo']['name'];
    $file_type = $_FILES['photo']['type'];
    $file_size = $_FILES['photo']['size'];
    $file_tname = $_FILES['photo']['tmp_name'];
    $file_error = $_FILES['photo']['error'];
    $image_folder = "images/";
    
    if(($file_type== "image/gif")||($file_type== "image/jpeg")||($file_type=="image/pjpeg")||($file_type=="image/png"||($file_type=="application/pdf")))
    { 
      if ($file_error > 0)
      {
        echo "Return Code: " .$file_error. "<br />";
      }
      else
      {
        // $imgfile = file_get_contents($file_tname);
        $img = file_get_contents($file_tname);
        $base64 = base64_encode($img);
    //    echo '<table><tr><td><img src="data:image/gif;base64,'. $base64 .'" width="200" height="200"/></td><tr>';
    //    echo "            
    //    <tr>
    //    <td>Image Name</td>
    //    <td>".$file_name."</td>
    //    </tr>";
    //    echo"
    //    <tr>
    //    <td>Image Size </td>
    //    <td>".($file_size/1000)."KB</td>
    //    </tr>";
        if($base64 != null){
          if($_POST['outputs'] == "raw") {
            header("location:index.php");
            $url = 'https://jkk9sonp1h.execute-api.ap-southeast-1.amazonaws.com/default/textract-textraw';
            $ch = curl_init($url);
            $data = array(
              'Image' => $base64
            );
            $payload = json_encode($data);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
            curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type:application/json'));
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            $result = curl_exec($ch);
            $hasil = json_encode($result);
            curl_close($ch);

          }
          else if($_POST['outputs'] == "form") {
            header("location:index.php");
            $url = 'https://jkk9sonp1h.execute-api.ap-southeast-1.amazonaws.com/default/textractv2';
            $ch = curl_init($url);
            $data = array(
              'Image' => $base64
            );
            $payload = json_encode($data);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
            curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type:application/json'));
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            $result = curl_exec($ch);
            $hasil = json_encode($result);
            curl_close($ch);
          }
        }
        echo "<script>addData('$result');</script>"; 
            
      }
    }
    else
    {
      echo "file is not Supported";
    }
  }
?> 