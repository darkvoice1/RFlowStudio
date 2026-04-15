from math import exp, lgamma, log


class DatasetAnalysisStatsUtils:
    """提供统计分析服务共用的数值工具。"""

    @staticmethod
    def round_number(value: float) -> float | int:
        """把结果整理成更适合展示的数值。"""
        rounded_value = round(value, 4)
        if rounded_value.is_integer():
            return int(rounded_value)

        return rounded_value

    @staticmethod
    def regularized_beta(a: float, b: float, x: float) -> float:
        """计算正则化不完全 Beta 函数。"""
        if x <= 0:
            return 0.0
        if x >= 1:
            return 1.0

        log_term = (
            lgamma(a + b)
            - lgamma(a)
            - lgamma(b)
            + a * log(x)
            + b * log(1 - x)
        )
        beta_factor = exp(log_term)

        if x < (a + 1) / (a + b + 2):
            return beta_factor * DatasetAnalysisStatsUtils._beta_fraction(a, b, x) / a

        return 1 - (
            beta_factor
            * DatasetAnalysisStatsUtils._beta_fraction(b, a, 1 - x)
            / b
        )

    @staticmethod
    def student_t_two_tailed_p_value(t_statistic: float, degrees_of_freedom: int) -> float:
        """计算双尾 t 检验 p 值。"""
        if degrees_of_freedom <= 0:
            return 1.0

        absolute_t = abs(t_statistic)
        x = degrees_of_freedom / (degrees_of_freedom + absolute_t * absolute_t)
        return DatasetAnalysisStatsUtils.regularized_beta(
            degrees_of_freedom / 2,
            0.5,
            x,
        )

    @staticmethod
    def f_survival_p_value(f_statistic: float, numerator_df: int, denominator_df: int) -> float:
        """计算 F 分布右尾 p 值。"""
        if numerator_df <= 0 or denominator_df <= 0:
            return 1.0
        if f_statistic <= 0:
            return 1.0

        x = (
            numerator_df * f_statistic
            / (numerator_df * f_statistic + denominator_df)
        )
        cdf = DatasetAnalysisStatsUtils.regularized_beta(
            numerator_df / 2,
            denominator_df / 2,
            x,
        )
        return max(0.0, 1 - cdf)

    @staticmethod
    def chi_square_survival_p_value(chi_square: float, degrees_of_freedom: int) -> float:
        """计算卡方分布右尾 p 值。"""
        if degrees_of_freedom <= 0:
            return 1.0
        if chi_square <= 0:
            return 1.0

        shape = degrees_of_freedom / 2
        x = chi_square / 2
        return DatasetAnalysisStatsUtils._regularized_gamma_q(shape, x)

    @staticmethod
    def _beta_fraction(a: float, b: float, x: float) -> float:
        """用连分式逼近 Beta 函数内部项。"""
        max_iterations = 200
        epsilon = 3e-7
        fp_min = 1e-30
        qab = a + b
        qap = a + 1
        qam = a - 1
        c_value = 1.0
        d_value = 1 - qab * x / qap
        if abs(d_value) < fp_min:
            d_value = fp_min
        d_value = 1 / d_value
        fraction = d_value

        for iteration in range(1, max_iterations + 1):
            even_index = 2 * iteration
            numerator = (
                iteration
                * (b - iteration)
                * x
                / ((qam + even_index) * (a + even_index))
            )
            d_value = 1 + numerator * d_value
            if abs(d_value) < fp_min:
                d_value = fp_min
            c_value = 1 + numerator / c_value
            if abs(c_value) < fp_min:
                c_value = fp_min
            d_value = 1 / d_value
            fraction *= d_value * c_value

            numerator = (
                -(a + iteration)
                * (qab + iteration)
                * x
                / ((a + even_index) * (qap + even_index))
            )
            d_value = 1 + numerator * d_value
            if abs(d_value) < fp_min:
                d_value = fp_min
            c_value = 1 + numerator / c_value
            if abs(c_value) < fp_min:
                c_value = fp_min
            d_value = 1 / d_value
            delta = d_value * c_value
            fraction *= delta

            if abs(delta - 1) < epsilon:
                break

        return fraction

    @staticmethod
    def _regularized_gamma_q(shape: float, x: float) -> float:
        """计算正则化上不完全 Gamma 函数。"""
        if x <= 0:
            return 1.0

        if x < shape + 1:
            return 1 - DatasetAnalysisStatsUtils._regularized_gamma_p_series(shape, x)

        return DatasetAnalysisStatsUtils._regularized_gamma_q_fraction(shape, x)

    @staticmethod
    def _regularized_gamma_p_series(shape: float, x: float) -> float:
        """用级数展开逼近正则化下不完全 Gamma 函数。"""
        max_iterations = 200
        epsilon = 3e-7
        term = 1 / shape
        total = term

        for iteration in range(1, max_iterations + 1):
            term *= x / (shape + iteration)
            total += term
            if abs(term) < abs(total) * epsilon:
                break

        return total * exp(-x + shape * log(x) - lgamma(shape))

    @staticmethod
    def _regularized_gamma_q_fraction(shape: float, x: float) -> float:
        """用连分式逼近正则化上不完全 Gamma 函数。"""
        max_iterations = 200
        epsilon = 3e-7
        fp_min = 1e-30
        b_value = x + 1 - shape
        c_value = 1 / fp_min
        d_value = 1 / b_value
        fraction = d_value

        for iteration in range(1, max_iterations + 1):
            numerator = -iteration * (iteration - shape)
            b_value += 2
            d_value = numerator * d_value + b_value
            if abs(d_value) < fp_min:
                d_value = fp_min
            c_value = b_value + numerator / c_value
            if abs(c_value) < fp_min:
                c_value = fp_min
            d_value = 1 / d_value
            delta = d_value * c_value
            fraction *= delta

            if abs(delta - 1) < epsilon:
                break

        return fraction * exp(-x + shape * log(x) - lgamma(shape))
