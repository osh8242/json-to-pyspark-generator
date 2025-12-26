// 파일: src/main/java/com/douzone/platform/recipe/step/StepRegistry.java
package com.douzone.platform.recipe.step;

import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.step.handlers.*;
import com.douzone.platform.recipe.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * node -> handler 매핑(명시 등록 버전).
 * - SPI(ServiceLoader)로 확장하고 싶으면 여기만 교체하면 됨.
 */
public class StepRegistry {

    private final List<StepHandler> handlers = new ArrayList<>();
    private final StepHandler defaultHandler;

    private StepRegistry(StepHandler defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    public static StepRegistry defaultRegistry() {
        StepRegistry r = new StepRegistry(new DefaultTransformStepHandler());

        // SOURCE
        r.register(new LoadStepHandler());

        // DF_TRANSFORM (대부분)
        r.register(TransformStepHandler.defaultTransformHandler());

        // ACTION_VOID
        r.register(ActionStatementStepHandler.defaultActionHandler());

        // ACTION_SCALAR
        r.register(new CountStepHandler());
        r.register(new FirstStepHandler());
        r.register(new IsEmptyStepHandler());

        return r;
    }

    public void register(StepHandler h) {
        handlers.add(h);
    }

    public StepHandler get(String node) {
        for (StepHandler h : handlers) {
            if (h.matches(node)) return h;
        }
        return defaultHandler; // 기존 buildDefaultStep 호환
    }

    /**
     * 중앙 검증:
     * - output REQUIRED/OPTIONAL/FORBIDDEN
     * - persist ALLOWED/FORBIDDEN
     * - mode 지원 여부
     */
    public void validate(StepRequest req, StepHandler handler) {
        if (!handler.supports(req.getMode())) {
            throw new RecipeStepException("Step not allowed in mode=" + req.getMode() + ", node=" + req.getNode());
        }

        // output policy
        OutputPolicy op = handler.outputPolicy();
        boolean hasOut = StringUtil.hasText(req.getOutput());

        if (op == OutputPolicy.REQUIRED && !hasOut) {
            throw new RecipeStepException("Step requires non-empty 'output'. node=" + req.getNode());
        }
        if (op == OutputPolicy.FORBIDDEN && hasOut) {
            throw new RecipeStepException("Step forbids 'output'. node=" + req.getNode());
        }

        // persist policy
        if (req.isPersist() && handler.persistPolicy() == PersistPolicy.FORBIDDEN) {
            throw new RecipeStepException("Step forbids 'persist'. node=" + req.getNode());
        }
    }
}
