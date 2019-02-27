#ARMA
library(MASS)
library(quadprog)
library(ggplot2)

install.packages("Mcomp")
library(openxlsx)
library(readxl)

require(smooth)
require(Mcomp)
require(zoo)
setwd("/Users/binsair/Desktop/FT/interest rate trend/data over past 1 year")
#data = read.xlsx("total_loan_avg_rate.xlsx", sheet = 1, skipEmptyRows = TRUE, colNames = TRUE)
data = read_excel("total_loan_avg_rate.xlsx",col_names = TRUE)
rate = data$Rate
date = data$Date
par(mfrow=c(1,1))

rate_ma7 = rollmean(rate, 7)
#### ACF Test
acf(rate_ma7, lag.max=100, plot = TRUE, main = "P2P loan_rate ACF")

#### PACF Test
pacf(rate_ma7, lag.max=100, plot = TRUE, main = "P2P loan_rate PACF")##ARMA process

#### Stationary Test
#install.packages('TTR')
#install.packages('tseries', dependencies=TRUE)


##unit-root test---stationary
#The Augmented Dickey–Fuller (ADF) t-statistic test: small p-values suggest the data is stationary and doesn’t need to be differenced stationarity.
library(tseries) 
adf.test(rate_ma7)
#The Kwiatkowski-Phillips-Schmidt-Shin (KPSS) test; here accepting the null hypothesis means that the series is stationarity, and small p-values suggest that the series is not stationary and a differencing is required.
kpss.test(rate_ma7)
#Computes the Phillips-Perron test for the null hypothesis that x has a unit root against a stationary alternative.
pp.test(rate_ma7)
diff(rate_ma7,7)
plot(rate_ma7,type="l")
##In a word, rate_ma7 is not stationary

n <- length(rate_ma7);
lrate <- log(rate_ma7[-1]/rate_ma7[-n])

acf(lrate, lag.max=100, plot = TRUE, main = "P2P loan_rate ACF")
pacf(lrate, lag.max=100, plot = TRUE, main = "P2P loan_rate PACF")##ARMA process



plot(rate_diff_1,type="l")


#The Augmented Dickey–Fuller (ADF) t-statistic test: small p-values suggest the data is stationary and doesn’t need to be differenced stationarity.
library(tseries) 
adf.test(lrate)
#The Kwiatkowski-Phillips-Schmidt-Shin (KPSS) test; here accepting the null hypothesis means that the series is stationarity, and small p-values suggest that the series is not stationary and a differencing is required.
kpss.test(lrate)
#Computes the Phillips-Perron test for the null hypothesis that x has a unit root against a stationary alternative.
pp.test(lrate)

#ARIMA(p,0,q) selection with min(AIC)

par(mfrow=c(1,2))
library(forecast)
fit1 = auto.arima(lrate,d=0,max.p=10,max.q=10,max.order=10,ic="aic",
                  seasonal=FALSE,stepwise=FALSE,trace=TRUE,
                  approximation=FALSE,allowdrift=FALSE,allowmean=FALSE)


fit1


##AR(8) process


##AR(8)+GARCH(1,1)
library("tseries")
#install.packages('fGarch')
library("fGarch")
garch(sp,order=c(1,1))
##sigma(t)^2 = 5.244e-06 + 0.179 * sigma(t-1)^2 + 0.725 * X(t-1)^2

fit2 = garchFit(formula = ~ arma(8,0) + garch(1,1), data = lrate, cond.dist = 'norm')
#sink('GARCH(1,1)-output.txt')
summary(fit2)
#sink()
