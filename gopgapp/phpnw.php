<?php
    $db = new PDO("pgsql:host=localhost;dbname=northwind;port=5432", "postgres", "toor");

    $res = $db->query("Select * From products");

    $totalRowCount = 0;

    foreach ($res as $row) {
        $totalRowCount++;
        printf("%5d | %-40.40s | %12.2f\n", $row["ProductID"], htmlentities($row['ProductName'],ENT_QUOTES,'UTF-8'), $row['UnitPrice']);
    }

    $custsth = $db->prepare('Select "CustomerID", "CompanyName", "City", "Country" from customers');
    $custsth->execute();
    foreach ($custsth->fetchAll() as $cust) {
        $totalRowCount++;
        printf("%-6s | %-40.40s | %-20.20s | %-20.20s\n", $cust['CustomerID'], htmlentities($cust['CompanyName']), htmlentities($cust['City']), htmlentities($cust['Country']));
    }
    
    $ordsth = $db->prepare('Select "CustomerID", "OrderDate", "RequiredDate", "ShippedDate" from orders');
    $ordsth->execute();
    foreach ($ordsth->fetchAll() as $order) {
        $totalRowCount++;
        echo $order['CustomerID'], ' | ', $order['OrderDate'], ' | ', $order["RequiredDate"], ' | ', $order['ShippedDate'], "\n";
    }

    $ordetsth = $db->prepare('Select od."OrderID", p."ProductName", od."Quantity", od."UnitPrice" from order_details od Join products p on p."ProductID" = od."ProductID"');

    $ordetsth->execute();
    $oldOrderid = '';
    
    echo "\n\nOrder Details\n\n";

    foreach ($ordetsth->fetchAll() as $od) {
        $totalRowCount++;
        if ($od['OrderID'] != $oldOrderid) {
            $oldOrderid = $od['OrderID'];
            echo $od['OrderID'], "\n";
        }
        printf("\t%-40.40s | %8d | %12.2f\n", htmlentities($od['ProductName']), $od["Quantity"], $od['UnitPrice']);
    }

    echo "\n\nCustomer wise Order Details\n\n";

    $ordsth = $db->prepare('Select "OrderID", to_char("OrderDate",\'dd-mm-yyyy\') "OrderDate", to_char("ShippedDate",\'dd-mm-yyyy\') "ShippedDate" from orders Where "CustomerID" = ?');
    $ordetsth = $db->prepare('Select od."OrderID", p."ProductName", od."Quantity", od."UnitPrice" from order_details od Join products p on p."ProductID" = od."ProductID" Where "OrderID" = ?');

    $custsth->execute();

    foreach ($custsth->fetchAll() as $cust) {
        $totalRowCount++;
        $custid = $cust['CustomerID'];
        echo "\n", $custid, ' -> ', htmlentities($cust['CompanyName']), ', ', htmlentities($cust['City']), ', ', htmlentities($cust['Country']), "\n\n";

        $ordsth->execute([$custid]);

        foreach ($ordsth->fetchAll() as $ord) {
            $totalRowCount++;
            echo "\n\t", $ord["OrderID"], ' - ', $ord["OrderDate"], ' - ', $ord["ShippedDate"], "\n\n";
            $ordetsth->execute([$ord["OrderID"]]);
            
            foreach($ordetsth->fetchAll() as $od) {
                $totalRowCount++;
                printf("\t\t%-40s | %8d | %12.2f\n", htmlentities($od['ProductName']), $od["Quantity"], $od['UnitPrice']);
            }
        }

    }

    echo "\n\nTotal rows fetched: ", $totalRowCount, "\n";
        
