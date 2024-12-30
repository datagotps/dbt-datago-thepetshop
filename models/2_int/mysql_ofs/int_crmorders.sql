WITH cte_coupon AS (
    SELECT
        WebOrderNo,
        STRING_AGG(DISTINCT CouponCode, ',') AS CouponCode,
        SUM(couponamount) AS couponamount
    FROM {{ ref('stg_inboundpaymentline') }}
    WHERE
        CouponCode != ''
        AND IsHeader = 0
    GROUP BY
        WebOrderNo
)

SELECT DISTINCT
    A.WebOrderNo AS OrderNo,
    A.OrderDateTime AS OrderDate,
    A.CustomerComment AS CustomerComment,
    B.city AS Region,
    B.Street AS Street,
    C.LastName AS LastName,
    B.Region AS City,
    B.State AS Emirate,
    B.Latitude,
    B.Longitude,
    CASE
        WHEN C.OrderSource = 'D' THEN 'Website'
        WHEN C.OrderSource IN ('CRM', 'CRM Exchange', 'FOC') THEN 'CRM'
        WHEN C.OrderSource = 'A' THEN 'Android'
        WHEN C.OrderSource = 'I' THEN 'iOS'
        ELSE C.OrderSource
    END AS Platform,
    CASE
        WHEN C.OrderSource = 'D' THEN 'Website'
        WHEN C.OrderSource IN ('CRM', 'CRM Exchange', 'FOC') THEN 'CRM'
        WHEN C.OrderSource IN ('A', 'I') THEN 'Mobile App'
        ELSE C.OrderSource
    END AS OrderSource,
    A.ordertype AS delivery_type,
    C.OrderType,
    C.CustomerEmail,
    -- A.CustomerId AS CustomerId,  (Commented out in original)
    C.FirstName AS FirstName,
    C.CustomerPhone AS PhoneNumber,
    C.OrderStatus AS OrderStatusId,
    D.StatusName AS OrderStatus,
    A.PaymentGateway AS PaymentMethod,
    A.ReservedField1 AS Area,
    A.ReservedField2,
    A.ReservedField3,
    cte_coupon.CouponCode AS CouponCode,
    A.ReservedField5,
    E.AmountInclTax,
    E.Discount,
    E.ShippingCharges,
    E.CODCharges,
    E.GiftCharges,
    E.Amount,
    E.Tax,
    cte_coupon.couponamount AS CouponAmount,
    E.CustomizedCharges,
    E.StoreCredit,
    E.OtherCharges,
    E.Loyaltypoints,
    E.LoyalityPointAmount,
    C.PackagingLocation,
    CASE
        WHEN C.PackagingLocation = 4 THEN 'OnlineDelivery'
        ELSE 'Click&Collect'
    END AS OrderType2

FROM {{ ref('stg_crmorders') }} AS C
LEFT JOIN {{ ref('stg_inboundsalesheader') }} AS A ON A.WebOrderNo = C.WebOrderNo
LEFT JOIN {{ ref('stg_inboundorderaddress') }} AS B ON A.WebOrderNo = B.WebOrderNo and B.AddressDetailType = 'Ship'
LEFT JOIN {{ ref('stg_orderstatusmaster') }} AS D ON C.OrderStatus = D.Id
LEFT JOIN {{ ref('stg_inboundpaymentline') }} AS E ON A.WebOrderNo = E.WebOrderNo and E.IsHeader = 1
LEFT JOIN cte_coupon ON A.WebOrderNo = cte_coupon.WebOrderNo


WHERE
    A.OrderDateTime >= '2021-07-01'
